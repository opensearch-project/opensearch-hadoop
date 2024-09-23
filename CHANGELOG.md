# CHANGELOG
Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
### Added
- Added basic support for HTTP compression when writing to OpenSearch ([#451](https://github.com/opensearch-project/opensearch-hadoop/pull/451))
- Added support for k-nn vectors ([#424](https://github.com/opensearch-project/opensearch-hadoop/pull/489))

### Changed

### Deprecated

### Removed
- Removed obsolete backported classes for Jackson 1.5 support ([#471](https://github.com/opensearch-project/opensearch-hadoop/pull/471))

### Fixed
- Remove unnecessary outputCommitter setting ([#465](https://github.com/opensearch-project/opensearch-hadoop/pull/465))

### Security

### Dependencies
- Bumps `com.fasterxml.jackson.core:jackson-annotations` from 2.17.0 to 2.17.2
- Bumps `com.fasterxml.jackson.core:jackson-databind` from 2.17.0 to 2.17.2
- Bumps `commons-logging:commons-logging` from 1.3.1 to 1.3.4
- Bumps `com.google.protobuf:protobuf-java` from 4.26.1 to 4.28.2
- Bumps `io.netty:netty-all` from 4.1.109.Final to 4.1.112.Final
- Bumps `jakarta.servlet:jakarta.servlet-api` from 6.0.0 to 6.1.0
- Bumps `commons-codec:commons-codec` from 1.17.0 to 1.17.1
- Bumps `org.slf4j:slf4j-api` from 2.0.13 to 2.0.16

## [1.2.0]
### Changed
- Changes invalid request exception messages to always be detailed ([#438](https://github.com/opensearch-project/opensearch-hadoop/pull/438))

### Fixed
- Fixes AWS SigV4 signing of `POST` requests with empty bodies such as scroll ([#443](https://github.com/opensearch-project/opensearch-hadoop/pull/443))

### Dependencies
- Bumps `com.fasterxml.jackson.core:jackson-databind` from 2.16.1 to 2.17.0
- Bumps `com.fasterxml.jackson.core:jackson-annotations` from 2.16.1 to 2.17.0
- Bumps `commons-logging:commons-logging` from 1.3.0 to 1.3.1
- Bumps `com.google.protobuf:protobuf-java` from 3.25.3 to 4.26.1
- Bumps `io.netty:netty-all` from 4.1.107.Final to 4.1.109.Final
- Bumps `org.slf4j:slf4j-api` from 2.0.12 to 2.0.13
- Bumps `commons-codec:commons-codec` from 1.16.1 to 1.17.0

## [1.1.0]
### Changed
- Update Gradle to 8.5 ([#377](https://github.com/opensearch-project/opensearch-hadoop/pull/377), [#386](https://github.com/opensearch-project/opensearch-hadoop/pull/386))

### Fixed
- Corrected the delete by query endpoint to match the OpenSearch API ([#350](https://github.com/opensearch-project/opensearch-hadoop/pull/350))

### Dependencies
- Bumps `com.google.protobuf:protobuf-java` from 3.22.3 to 3.25.3
- Bumps `com.esotericsoftware.kryo:kryo` from 2.21 to 2.24.0
- Bumps `io.netty:netty-all` from 4.1.92.Final to 4.1.107.Final
- Bumps `org.codehaus.woodstox:stax2-api` from 3.1.4 to 4.2.2
- Bumps `com.fasterxml.jackson.core:jackson-annotations` from 2.15.0 to 2.16.1
- Bumps `org.slf4j:slf4j-api` from 1.7.6 to 2.0.12
- Bumps `com.google.code.findbugs:jsr305` from 2.0.1 to 3.0.2
- Bumps `org.apache.htrace:htrace-core4` from 4.1.0-incubating to 4.2.0-incubating
- Bumps `com.fasterxml.jackson.core:jackson-databind` from 2.15.0 to 2.16.1
- Bumps `org.json4s:json4s-ast_2.10` from 3.2.10 to 3.6.12
- Bumps `org.apache.hadoop.thirdparty:hadoop-shaded-protobuf_3_7` from 1.0.0 to 1.1.1
- Bumps `org.apache.avro:avro` from 1.7.7 to 1.8.2
- Bumps `com.amazonaws:aws-java-sdk-bundle` from 1.12.451 to 1.12.651
- Bumps `commons-codec:commons-codec` from 1.15 to 1.16.1
- Bumps `com.amazonaws:aws-java-sdk-bundle` from 1.12.451 to 1.12.524
- Bumps `commons-logging:commons-logging` from 1.2 to 1.3.0
- Bumps `org.json4s:json4s-jackson_2.12` from 4.0.6 to 4.0.7
- Bumps `com.fasterxml.jackson.module:jackson-module-scala_2.12` from 2.15.2 to 2.16.1

## [1.0.1] - 05/30/2023
### Dependencies
- Bumps `commons-logging:commons-logging` from 1.1.1 to 1.2

### Fixed
- Fixes imports from being thirdparty to local ([#245](https://github.com/opensearch-project/opensearch-java/pull/245))

## [1.0.0] - 05/18/2023
### Added
- Added CHANGELOG and verifier workflow ([65](https://github.com/opensearch-project/opensearch-hadoop/pull/65))
- Added snapshot publication workflow ([218](https://github.com/opensearch-project/opensearch-hadoop/pull/218))
- Added release workflow ([227](https://github.com/opensearch-project/opensearch-hadoop/pull/227))

### Changed
- [Spark Distribution] Default Assemble artifact to Spark 3 ([107](https://github.com/opensearch-project/opensearch-hadoop/pull/107))
- Changed the default deserialization/serialization logic from Object based to JSON based ([154](https://github.com/opensearch-project/opensearch-hadoop/pull/154))

### Fixed
- Restored skipped push down tests ([125](https://github.com/opensearch-project/opensearch-hadoop/pull/125))
- Fixed spark failures due to deserialization failed logic ([219](https://github.com/opensearch-project/opensearch-hadoop/pull/219))

### Dependencies
- Bumps `com.google.guava:guava` from 16.0.1 to 23.0
- Bumps `com.google.guava:guava` from 11.0 to 23.0
- Bumps `commons-codec:commons-codec` from 1.4 to 1.15
- Bumps `com.google.code.findbugs:jsr305` from 2.0.1 to 3.0.2
- Bumps `jakarta.servlet:jakarta.servlet-api` from 4.0.3 to 6.0.0
- Bumps `com.fasterxml.jackson.core:jackson-databind` from 2.7.8 to 2.15.0
- Bumps `commons-httpclient:commons-httpclient` from 3.0.1 to 3.1
- Bumps `org.apache.rat:apache-rat` from 0.13 to 0.15
- Bumps `com.esotericsoftware.kryo:kryo` from 2.21 to 2.24.0
- Bumps `org.codehaus.woodstox:stax2-api` from 3.1.4 to 4.2.1
- Bumps `org.apache.hadoop.thirdparty:hadoop-shaded-protobuf_3_7` from 1.0.0 to 1.1.1
- Bumps `com.fasterxml.jackson.core:jackson-annotations` from 2.6.7 to 2.15.0
- Bumps `org.json4s:json4s-ast_2.10` from 3.2.10 to 3.6.12
- Bumps `commons-logging:commons-logging` from 1.1.1 to 1.2
- Bumps `com.amazonaws:aws-java-sdk-bundle` from 1.12.397 to 1.12.451
- Bumps `org.slf4j:slf4j-api` from 1.7.6 to 1.7.36
- Bumps `com.google.protobuf:protobuf-java` from 2.5.0 to 3.22.3
- Bumps `io.netty:netty-all` from 4.0.29.Final to 4.1.92.Final
- Bumps `jline:jline` from 0.9.94 to 1.0
- Bumps `org.json4s:json4s-jackson_2.12` from 3.2.11 to 4.0.6
- Bumps `org.apache.avro:avro` from 1.7.7 to 1.11.1

[Unreleased]: https://github.com/opensearch-project/opensearch-hadoop/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/opensearch-project/opensearch-hadoop/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/opensearch-project/opensearch-hadoop/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/opensearch-project/opensearch-hadoop/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/opensearch-project/opensearch-hadoop/compare/v7.13.4...v1.0.0