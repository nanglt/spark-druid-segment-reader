# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2025-11-27

### Breaking Changes
- **Upgraded to Apache Spark 3.5.6** from Spark 2.4.8
- **Upgraded to Scala 2.12.18** from Scala 2.11.12
- **Minimum Java version is now 11** (previously Java 8)
- Migrated from deprecated DataSource V2 API to new Spark 3.x Connector API

### Added
- Support for Apache Spark 3.5.6
- Support for Apache Druid 28.0.0
- Support for Hadoop 3.3.6
- New build scripts (`build.sh` and `build.bat`)
- Enhanced compiler options for better code quality
- Migration guide for upgrading from v0.x

### Changed
- Replaced `DataSourceV2` with `TableProvider`
- Replaced `DataSourceReader` with `Batch` interface
- Replaced `InputPartitionReader` with `PartitionReader`
- Replaced deprecated `JavaConverters` with `CollectionConverters`
- Updated SBT to 1.10.6
- Updated sbt-assembly plugin to 2.3.0
- Improved error handling and logging
- Better memory management in partition readers

### Removed
- Removed Spark 2.4.x compatibility layer
- Removed custom `SerializableHadoopConfiguration` (now using built-in version)
- Removed unused Spark modules (spark-unsafe, spark-graphx)

### Fixed
- Schema inference performance improvements
- Better handling of mixed granularity segments
- Improved thread safety in segment readers

## [0.4.0-SNAPSHOT] - Previous Release
- Support for Apache Spark 2.4.8
- Support for reading Druid segments from S3 and HDFS
- Automatic segment version detection
- Schema merging across multiple segments

