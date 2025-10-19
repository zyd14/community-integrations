# Changelog

## [Unreleased]

### Added

- Spark I/O manager.
- Support for append mode in Iceberg I/O manager. Write mode options can be set via asset definition metadata using the `write_mode` key, or at runtime via output metadata with the same key. Runtime output metadata setting overrides asset definition metadata setting.

### Changed

- Rename I/O managers from `<Storage><Engine>IOManager` to `<Engine><Storage>IOManager`.
- Support for pyiceberg 0.10.0. Change behavior for how partition field names are calculated to address validation introduced in [pyiceberg #2505](https://github.com/apache/iceberg-python/pull/2305). Partition field names calculated for Dagster asset partitions now include a configurable prefix before the column name they reference.
