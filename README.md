[![Build and test MR](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_mr.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_mr.yml)
[![Build and test spark](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark.yml)
[![Build and test spark 30](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_30.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_30.yml)
[![Build and test spark 35](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_35.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_35.yml)
[![Build and test spark 40](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_40.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_40.yml)
[![Build and test hive](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_hive.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_hive.yml)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

![OpenSearch logo](OpenSearch.svg)

# OpenSearch Hadoop
OpenSearch real-time search and analytics natively integrated with Hadoop.
Supports [Map/Reduce](#mapreduce), [Apache Hive](#apache-hive), [Apache Spark](#apache-spark).

- [OpenSearch Hadoop](#opensearch-hadoop)
  - [Requirements](#requirements)
  - [Usage](#usage)
  - [Compatibility](#compatibility)
  - [Building the source](#building-the-source)
  - [License](#license)

## Requirements
OpenSearch (__1.3.x__ or higher) cluster accessible through REST. That's it!
If using SigV4 IAM auth features, you would need to include the following AWS SDK v2 dependencies in your job classpath:
- `software.amazon.awssdk:auth:2.31.59` (or later)
- `software.amazon.awssdk:regions:2.31.59` (or later)
- `software.amazon.awssdk:http-client-spi:2.31.59` (or later)
- `software.amazon.awssdk:identity-spi:2.31.59` (or later)
- `software.amazon.awssdk:sdk-core:2.31.59` (or later)
- `software.amazon.awssdk:utils:2.31.59` (or later)

## Usage

Please see the [USER_GUIDE](USER_GUIDE.md) for usage.

## Compatibility

See [Compatibility](COMPATIBILITY.md).

## Building the source

OpenSearch Hadoop uses [Gradle][] for its build system and it is not required to have it installed on your machine. By default (`gradlew`), it automatically builds the package and runs the unit tests. For integration testing, use the `integrationTests` task.
See `gradlew tasks` for more information.

To create a distributable zip, run `gradlew distZip` from the command line; once completed you will find the jar in `build/libs`.

To build the project, JVM 8, JVM 11, and JVM 17 are required. The minimum compiler version is Java 14 and the minimum runtime is Java 8.

## License
This project is released under version 2.0 of the [Apache License][]

```
Licensed to Elasticsearch under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. Elasticsearch licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
```

[Hadoop]: http://hadoop.apache.org
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Gradle]: http://www.gradle.org/
