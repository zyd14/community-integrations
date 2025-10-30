# Features

## Supported catalog backends

`dagster-iceberg` supports all catalog backends that are available through `pyiceberg`. See overview and configuration options [here](https://py.iceberg.apache.org/configuration/#catalogs).

## Configuration

`dagster-iceberg` supports setting configuration values using a `.pyiceberg.yaml` configuration file and environment variables. For more information, see the [PyIceberg documentation](https://py.iceberg.apache.org/configuration/#setting-configuration-values).

You may also pass your catalog configuration through use of the `IcebergCatalogConfig` object, e.g:

```python
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager

io_manager = PyArrowIcebergIOManager(
    name=catalog_name,
    config=IcebergCatalogConfig(properties={
        "uri": "postgresql+psycopg2://pyiceberg:pyiceberg@postgres/catalog",
        "warehouse": f"file://path/to/warehouse",
    }),
    namespace=namespace,
)
```

## Implemented engines

The following engines are currently implemented.

- [arrow](https://arrow.apache.org/docs/python/index.html)
- [daft](https://www.getdaft.io/)
- [pandas](https://pandas.pydata.org/)
- [polars](https://pola.rs/)

## PyIceberg Features

The table below shows which PyIceberg features are currently available.

| Feature | Supported | Link | Comment |
|---|---|---|---|
| Add existing files | ❌ | https://py.iceberg.apache.org/api/#add-files | Useful for existing partitions that users don't want to re-materialize/re-compute. |
| Schema evolution | ✅ | https://py.iceberg.apache.org/api/#schema-evolution | More complicated than e.g. delta lake since updates require diffing input table with existing Iceberg table. This is implemented by checking the schema of incoming data, dropping any columns that no longer exist in the data schema, and then using the `union_by_name()` method to merge the current schema with the table schema. Current implementation has a chance of creating a race condition when e.g. partition A tries to write to a table that has not yet processed a schema update. Should be covered by retrying when writing. |
| Sort order | ❌ | https://shorturl.at/TycZN | Currently limited support in PyIceberg. Sort ordering is supported when creating a table from an Iceberg schema (one must pass the source_id which can be inferred from a PyArrow schema but this is shaky). However, we cannot simply update a sort ordering like a partition or schema spec. |
| PyIceberg commit retries | ✅ | https://github.com/apache/iceberg-python/pull/330 https://github.com/apache/iceberg-python/issues/269 | PR to add this to PyIceberg is open. Will probably be merged for an upcoming release. Added a custom retry function using Tenacity for the time being. |
| Partition evolution | ✅ | https://py.iceberg.apache.org/api/#partition-evolution | Create, Update, Delete partitions by updating the Dagster partitions definition. |
| Table properties | ✅ | https://py.iceberg.apache.org/api/#table-properties | Added as metadata on an asset. NB: config options are not checked explicitly because users can add any key-value pair to a table. Available properties [here](https://py.iceberg.apache.org/configuration/#tables). |
| Snapshot properties | ✅ | https://py.iceberg.apache.org/api/#snapshot-properties | Useful for correlating Dagster runs to snapshots by adding tags to snapshot. Not configurable by end-user. |
| Upsert | ✅ | https://py.iceberg.apache.org/api/#upsert | Update existing rows and insert new rows in a single operation. Configure via `write_mode: "upsert"` and `upsert_options` in asset metadata. |
