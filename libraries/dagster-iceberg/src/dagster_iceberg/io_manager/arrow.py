from collections.abc import Sequence
from typing import Union

import pyarrow as pa
from dagster._annotations import public
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyiceberg import expressions as E
from pyiceberg import table as ibt

from dagster_iceberg import handler as _handler
from dagster_iceberg import io_manager as _io_manager
from dagster_iceberg._utils.partitions import DagsterPartitionToIcebergExpressionMapper
from dagster_iceberg._utils.preview import preview

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]  # noqa: UP007, avoid Pyright failure


class _PyArrowIcebergTypeHandler(_handler.IcebergBaseTypeHandler[ArrowTypes]):
    """Type handler that converts data between Iceberg tables and PyArrow tables."""

    def to_data_frame(
        self,
        table: ibt.Table,
        table_slice: TableSlice,
        target_type: type[ArrowTypes],
        snapshot_id: int | None = None,
    ) -> ArrowTypes:
        selected_fields: tuple[str, ...] = (
            tuple(table_slice.columns) if table_slice.columns is not None else ("*",)
        )
        row_filter: E.BooleanExpression
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToIcebergExpressionMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = E.And(*expressions) if len(expressions) > 1 else expressions[0]
        else:
            row_filter = ibt.ALWAYS_TRUE

        table_scan = table.scan(row_filter=row_filter, selected_fields=selected_fields, snapshot_id=snapshot_id)

        return (
            table_scan.to_arrow()
            if target_type == pa.Table
            else table_scan.to_arrow_batch_reader()
        )

    def to_arrow(self, obj: ArrowTypes) -> pa.Table:
        return obj

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return (pa.Table, pa.RecordBatchReader)


@public
@preview
class PyArrowIcebergIOManager(_io_manager.IcebergIOManager):
    """An I/O manager definition that reads inputs from and writes outputs to Iceberg tables using PyArrow.

    Examples:
        .. code-block:: python

            import pandas as pd
            import pyarrow as pa
            from dagster import Definitions, asset
            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": PyArrowIcebergIOManager(
                    name="test",
                    config=IcebergCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    namespace="dagster",
                )
            }


            @asset
            def iris_dataset() -> pa.Table:
                pa.Table.from_pandas(
                    pd.read_csv(
                        "https://docs.dagster.io/assets/iris.csv",
                        names=[
                            "sepal_length_cm",
                            "sepal_width_cm",
                            "petal_length_cm",
                            "petal_width_cm",
                            "species",
                        ],
                    )
                )


            defs = Definitions(assets=[iris_dataset], resources=resources)

        If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
        the I/O manager. For assets, the schema will be determined from the asset key, as in the above example.
        For ops, the schema can be specified by including a "schema" entry in output metadata. If none
        of these is provided, the schema will default to "public". The I/O manager will check if the namespace
        exists in the Iceberg catalog. It does not automatically create the namespace if it does not exist.

        .. code-block:: python

            @op(
                out={"my_table": Out(metadata={"schema": "my_schema"})}
            )
            def make_my_table() -> pa.Table:
                ...

        To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
        ``In`` or ``AssetIn``.

        .. code-block:: python

            @asset(
                ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
            )
            def my_table_a(my_table: pa.Table):
                # my_table will just contain the data from column "a"
                ...

    """

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [_PyArrowIcebergTypeHandler()]
