from typing import Any

from dagster import Config
from dagster._annotations import public
from pydantic import Field

from dagster_iceberg._utils.preview import preview
from pyiceberg.table.refs import MAIN_BRANCH


class IcebergBranchConfig(Config):
    """Configuration for Iceberg Branches.

    See the `Branches section <https://py.iceberg.apache.org/configuration/#branches>`_
    for configuration options.
    """
    # TODO: verify the descriptions correctly reflect how these parameters work
    ref_snapshot_id: int | None = Field(description="Snapshot ID of the reference to use as a base for the branch. If the specified snapshot ID does not exist an error will be raised. If not specified, the current snapshot will be used.")
    branch_name: str = Field(default=MAIN_BRANCH, description="Name of the branch to use. If the specified branch does not yet exist it will be created.")
    max_snapshot_age_ms: int | None = Field(description="Maximum age of a snapshot in milliseconds. If a snapshot is older than this age, it will be deleted.")
    min_snapshots_to_keep: int | None = Field(description="Minimum number of snapshots to keep. If the number of snapshots exceeds this number, the oldest snapshot will be deleted.")
    max_ref_age_ms: int | None = Field(description="Maximum age of a reference in milliseconds. If a reference is older than this age, it will be deleted.")


@public
@preview
class IcebergCatalogConfig(Config):
    """Configuration for Iceberg Catalogs.

    See the `Catalogs section <https://py.iceberg.apache.org/configuration/#catalogs>`_
    for configuration options.

    You can configure the Iceberg IO manager:

        1. Using a ``.pyiceberg.yaml`` configuration file.
        2. Through environment variables.
        3. Using the ``IcebergCatalogConfig`` configuration object.

    For more information about the first two configuration options, see
    `Setting Configuration Values <https://py.iceberg.apache.org/configuration/#setting-configuration-values>`_.

    Example:
        .. code-block:: python

            from dagster_iceberg.config import IcebergCatalogConfig
            from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

            warehouse_path = "/path/to/warehouse"

            io_manager = PyArrowIcebergIOManager(
                name="my_catalog",
                config=IcebergCatalogConfig(
                    properties={
                        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                        "warehouse": f"file://{warehouse_path}",
                    }
                ),
                namespace="my_namespace",
            )

    """

    properties: dict[str, Any]
    branch_config: IcebergBranchConfig = Field(default_factory=IcebergBranchConfig, description="Configuration for Iceberg table branch. If the specified branch does not yet exist it will be created.")
    error_if_branch_and_no_snapshots: bool = Field(default=False, description="Whether to error if a branch is specified but no snapshots exist on the table. If False, a warning will be logged and the write will proceed to the main branch.")