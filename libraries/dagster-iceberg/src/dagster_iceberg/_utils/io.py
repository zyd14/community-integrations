import logging
from collections.abc import Sequence
from enum import Enum
from typing import Final

import pyarrow as pa
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg import __version__ as pyiceberg_version
from pyiceberg import expressions as E
from pyiceberg import table as iceberg_table
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema

from dagster_iceberg._utils.partitions import (
    DagsterPartitionToIcebergExpressionMapper,
    update_table_partition_spec,
)
from dagster_iceberg._utils.properties import update_table_properties
from dagster_iceberg._utils.retries import IcebergOperationWithRetry
from dagster_iceberg._utils.schema import update_table_schema
from dagster_iceberg.config import IcebergBranchConfig
from dagster_iceberg.version import __version__ as dagster_iceberg_version

from pyiceberg.table.refs import MAIN_BRANCH

logger = logging.getLogger("dagster_iceberg._utils.io")


class WriteMode(Enum):
    append = "append"
    overwrite = "overwrite"


DEFAULT_WRITE_MODE: Final[WriteMode] = WriteMode.overwrite


def table_writer(
    table_slice: TableSlice,
    data: pa.Table,
    catalog: Catalog,
    schema_update_mode: str,
    partition_spec_update_mode: str,
    dagster_run_id: str,
    branch_config: IcebergBranchConfig,
    dagster_partition_key: str | None = None,
    table_properties: dict[str, str] | None = None,
    write_mode: WriteMode = DEFAULT_WRITE_MODE,
) -> None:
    """Writes data to an iceberg table

    Args:
        table_slice (TableSlice): dagster database IO manager table slice. This
            contains information about dagster partitions.
        data (pa.Table): PyArrow table
        catalog (Catalog): PyIceberg catalogs supported by this library. See <https://py.iceberg.apache.org/configuration/#catalogs>.
        schema_update_mode (str): Whether to process schema updates on existing
            tables or error, value is either 'error' or 'update'
        partition_spec_update_mode (str): Whether to update partition spec when there are changes to the partitioning, or to throw an error. Value is either 'error' or 'update'
        dagster_run_id (str): Dagster run ID
        dagster_partition_key (str): Dagster partition key provided by asset execution context
        table_properties (dict[str, str]): Properties to set on the table. Will delete any existing properties that are not in table_properties, and update any existing properties.
        write_mode (WriteMode): Whether to append to the table or overwrite it. Value is either 'append' or 'overwrite'
        branch_config (IcebergBranchConfig): Configuration for Iceberg table branch. If the specified branch does not yet exist it will be created.

    Raises:
        ValueError: Raised when partition dimension metadata is not set on an
            asset but the user attempts to use partitition definitions.
        ValueError: Raised when schema update mode is set to 'error' and
            asset partition definitions on an existing table do not match
            the table partition spec.
    """
    table_path = f"{table_slice.schema}.{table_slice.table}"
    base_properties = {
        "created-by": "dagster",
        "dagster-run-id": dagster_run_id,
        "pyiceberg-version": pyiceberg_version,
        "dagster-iceberg-version": dagster_iceberg_version,
    }
    logger.debug(
        "Writing data to table %s with properties %s",
        table_path,
        base_properties,
    )
    # In practice, partition_dimensions is an empty list for unpartitioned assets and not None
    #  even though it's the default value.
    partition_exprs: list[str] | None = None
    partition_dimensions: Sequence[TablePartitionDimension] | None = None
    if table_slice.partition_dimensions:
        partition_exprs = [p.partition_expr for p in table_slice.partition_dimensions]
        if any(p is None for p in partition_exprs):
            raise ValueError(
                f"Could not map partition to partition expr, got '{partition_exprs}'."
                "Did you name your partitions correctly and provided the correct"
                " 'partition_expr' in the asset metadata?",
            )
        partition_dimensions = table_slice.partition_dimensions

    # TODO: may need to enable write.wap.enabled before doing branching stuff
    logger.debug("Partition dimensions: %s", partition_dimensions)
    if table_exists(catalog, table_path):
        logger.debug("Updating existing table")
        table = catalog.load_table(table_path)
        # Check if the table has partition dimensions set
        num_partition_fields = len(table.spec().fields)
        logger.debug(
            "Current table version has %s partition fields",
            num_partition_fields,
        )
        # Check if schema matches. If not, update
        update_table_schema(
            table=table,
            new_table_schema=data.schema,
            schema_update_mode=schema_update_mode,
        )
        # Check if partitions match. If not, update
        if (partition_dimensions is not None) or (num_partition_fields > 0):
            update_table_partition_spec(
                # Refresh metadata just in case a partition column was dropped
                table=table.refresh(),
                table_slice=table_slice,
                partition_spec_update_mode=partition_spec_update_mode,
            )
        if table_properties is not None:
            update_table_properties(
                table=table,
                current_table_properties=table.properties,
                new_table_properties=table_properties,
            )
    else:
        logger.debug("Creating new table")
        table = create_table(
            catalog=catalog,
            table_path=table_path,
            schema=data.schema,
            properties=(table_properties if table_properties is not None else {}),
        )
        if partition_dimensions is not None:
            update_table_partition_spec(
                table=table,
                table_slice=table_slice,
                # When creating new tables with dagster partitions, we always update
                # the partition spec
                partition_spec_update_mode="update",
            )

    row_filter: E.BooleanExpression
    if partition_dimensions is not None:
        row_filter = get_expression_row_filter(
            iceberg_table_schema=table.schema(),
            iceberg_partition_spec=table.spec(),
            dagster_partition_dimensions=partition_dimensions,
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    # For newly created tables, PyIceberg requires the first write to go to main branch
    # We'll create the branch after the first write
    first_table_write_and_branch_requested = False
    if branch_config.branch_name != MAIN_BRANCH:
        table_has_snapshots = table.current_snapshot() is not None
        if table_has_snapshots:
            create_branch_if_not_exists(table=table, branch_config=branch_config)
            branch_name = branch_config.branch_name
        else:
            logger.warning("Table has no snapshots, cannot write to branch %s until a snapshot is created on the %s branch. Writing to main branch instead. Subsequent writes will be on the %s branch.", branch_config.branch_name, MAIN_BRANCH, branch_config.branch_name)
            branch_name = MAIN_BRANCH
            first_table_write_and_branch_requested = True
    else:
        branch_name = MAIN_BRANCH

    snapshot_properties = (
        base_properties | {"dagster-partition-key": dagster_partition_key}
        if dagster_partition_key is not None
        else base_properties
    )

    logger.debug("Writing %d rows with mode=%s to branch=%s", len(data), write_mode, branch_name)
    if write_mode == WriteMode.append:
        append_to_table(
            table=table,
            data=data,
            snapshot_properties=snapshot_properties,
            branch_name=branch_name,
        )
    elif write_mode == WriteMode.overwrite:
        overwrite_table(
            table=table,
            data=data,
            overwrite_filter=row_filter,
            snapshot_properties=snapshot_properties,
            branch_name=branch_name,
        )
    else:
        raise ValueError(f"Unexpected write mode: {write_mode}")

    logger.debug("Write completed successfully")
    # Handle case where user had branch config but no snapshots on table
    # This allows subsequent table reads to use the requested branch even though the first write was to main branch
    if first_table_write_and_branch_requested:
        logger.debug("Creating branch %s after initial write", branch_config.branch_name)
        create_branch_if_not_exists(table=table, branch_config=branch_config)


def create_branch_if_not_exists(table: iceberg_table.Table, branch_config: IcebergBranchConfig):
    """Creates a branch if it does not exist"""
    # TODO: maybe we can defer committing until we actually write?
    # Check if branch already exists - refs() returns a dict with ref names as keys
    refs_dict = table.refs()
    logger.debug("Current table refs: %s", list(refs_dict.keys()))

    if branch_config.branch_name not in refs_dict:
        # If table has no snapshot yet (newly created), we can't create a branch from it
        current_snapshot = table.current_snapshot()
        if current_snapshot is None:
            logger.debug("Skipping branch creation for %s because table has no snapshots yet", branch_config.branch_name)
            return

        logger.debug("Creating branch %s from snapshot %s", branch_config.branch_name, current_snapshot.snapshot_id)
        ref_snapshot_id = branch_config.ref_snapshot_id if branch_config.ref_snapshot_id is not None else current_snapshot.snapshot_id
        with table.manage_snapshots() as ms:
            ms.create_branch(
                snapshot_id=ref_snapshot_id,
                branch_name=branch_config.branch_name,
                max_snapshot_age_ms=branch_config.max_snapshot_age_ms,
                min_snapshots_to_keep=branch_config.min_snapshots_to_keep,
                max_ref_age_ms=branch_config.max_ref_age_ms,
            )
        logger.debug("Branch %s created successfully", branch_config.branch_name)
    else:
        logger.debug("Branch %s already exists, skipping creation", branch_config.branch_name)


def get_expression_row_filter(
    iceberg_table_schema: Schema,
    iceberg_partition_spec: PartitionSpec,
    dagster_partition_dimensions: Sequence[TablePartitionDimension],
) -> E.BooleanExpression:
    """Construct an iceberg row filter based on dagster partition dimensions
    that can be used to overwrite those specific rows in the iceberg table."""
    partition_filters = DagsterPartitionToIcebergExpressionMapper(
        partition_dimensions=dagster_partition_dimensions,
        table_schema=iceberg_table_schema,
        table_partition_spec=iceberg_partition_spec,
    ).partition_dimensions_to_filters()
    return (
        E.And(*partition_filters)
        if len(partition_filters) > 1
        else partition_filters[0]
    )


def table_exists(catalog: Catalog, table_path: str) -> bool:
    """Checks if a table exists in the iceberg catalog

    NB: This is custom logic because for some reason, the PyIceberg REST implementation
    doesn't seem to work properly for catalog.table_exists(table_path). This is a workaround
    so that users don't run into strange issues when updating an existing table.

    Args:
        catalog (Catalog): PyIceberg catalogs supported by this library
        table_path (str): Table path

    Returns:
        bool: True if the table exists, False otherwise
    """
    try:
        catalog.load_table(table_path)
        return True
    except NoSuchTableError:
        return False


def create_table(
    catalog: Catalog,
    table_path: str,
    schema: pa.Schema,
    properties: dict[str, str],
) -> iceberg_table.Table:
    """Creates an iceberg table and retries on failure

    Args:
        catalog (Catalog): PyIceberg catalogs supported by this library. See <https://py.iceberg.apache.org/configuration/#catalogs>.
        table_path (str): Table path
        schema (pa.Schema): PyArrow schema
        properties (Dict[str, str]): Table properties

    Raises:
        RetryError: Raised when the commit fails after the maximum number of retries
    """
    IcebergCreateTableWithRetry(catalog=catalog).execute(
        retries=3,
        exception_types=(CommitFailedException, TableAlreadyExistsError),
        table_path=table_path,
        schema=schema,
        properties=properties,
    )
    return catalog.load_table(table_path)


class IcebergCreateTableWithRetry(IcebergOperationWithRetry):
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def refresh(self):
        """Not using this method because we don't have a table to refresh"""

    def operation(self, table_path: str, schema: pa.Schema, properties: dict[str, str]):
        self.catalog.create_table(
            table_path,
            schema=schema,
            properties=properties,
        )


def overwrite_table(
    table: iceberg_table.Table,
    data: pa.Table,
    overwrite_filter: E.BooleanExpression | str,
    snapshot_properties: dict[str, str] | None = None,
    branch_name: str | None = None,
):
    """Overwrites an iceberg table and retries on failure

    NB: This will be added in PyIceberg 0.8.0 or 0.9.0. This implementation is based
        on https://github.com/apache/iceberg-python/issues/269 and https://github.com/apache/iceberg-python/pull/330

    Args:
        table (table.Table): Iceberg table
        df (pa.Table): Data to write to the table
        overwrite_filter (Union[E.BooleanExpression, str]): Filter to apply to the overwrite operation
        snapshot_properties (dict[str, str]): Properties to set on the snapshot
        branch (str): Table branch to use. If the branch does not yet exist it will be created.

    Raises:
        RetryError: Raised when the commit fails after the maximum number of retries
    """
    IcebergTableOverwriterWithRetry(table=table).execute(
        retries=3,
        exception_types=CommitFailedException,
        data=data,
        overwrite_filter=overwrite_filter,
        snapshot_properties=snapshot_properties,
        branch_name=branch_name,
    )


def append_to_table(
    table: iceberg_table.Table,
    data: pa.Table,
    snapshot_properties: dict[str, str] | None = None,
    branch_name: str | None = None,
):
    """Appends data to an iceberg table and retries on failure

    Args:
        table (table.Table): Iceberg table
        data (pa.Table): Data to append to the table
        snapshot_properties (dict[str, str]): Properties to set on the snapshot
        branch (str): Table branch to use. If the branch does not yet exist it will be created.
    """
    IcebergTableAppenderWithRetry(table=table).execute(
        retries=3,
        exception_types=CommitFailedException,
        data=data,
        snapshot_properties=snapshot_properties,
        branch_name=branch_name,
    )


class IcebergTableAppenderWithRetry(IcebergOperationWithRetry):
    def operation(
        self,
        data: pa.Table,
        snapshot_properties: dict[str, str] | None = None,
        branch_name: str | None = None,
    ):
        if snapshot_properties is None:
            snapshot_properties = {}
        self.logger.debug("Appending %d rows to table %s (branch=%s)", len(data), self.table.name(), branch_name)
        self.table.append(
            df=data,
            snapshot_properties=snapshot_properties,
            branch=branch_name,
        )
        # Refresh table metadata to see the new snapshot
        self.table = self.table.refresh()
        self.logger.debug("Append completed, current snapshot after refresh: %s", self.table.current_snapshot())


class IcebergTableOverwriterWithRetry(IcebergOperationWithRetry):
    def operation(
        self,
        data: pa.Table,
        overwrite_filter: E.BooleanExpression | str,
        snapshot_properties: dict[str, str] | None = None,
        branch_name: str | None = None,
    ):
        self.logger.debug("Overwriting %d rows to table %s (branch=%s)", len(data), self.table.name(), branch_name)
        self.table.overwrite(
            df=data,
            overwrite_filter=overwrite_filter,
            snapshot_properties=(
                snapshot_properties if snapshot_properties is not None else {}
            ),
            branch=branch_name,
        )
        # Refresh table metadata to see the new snapshot
        self.table = self.table.refresh()
        self.logger.debug("Overwrite completed, current snapshot after refresh: %s", self.table.current_snapshot())
