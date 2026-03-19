[![Build and test MR](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_mr.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_mr.yml)
[![Build and test spark](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark.yml)
[![Build and test spark 30](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_30.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_30.yml)
[![Build and test spark 35](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_35.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_35.yml)
[![Build and test spark 40](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_40.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_spark_40.yml)
[![Build and test hive](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_hive.yml/badge.svg)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build_hive.yml)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

![OpenSearch logo](OpenSearch.svg)

# OpenSearch Hadoop

A connector for reading and writing data between [Apache Spark](http://spark.apache.org) and [OpenSearch](https://opensearch.org/). It enables Spark jobs to directly index data into OpenSearch and run queries against it, with parallel reads and writes across Spark partitions and OpenSearch shards for efficient distributed processing.

Also supports [Apache Hive](http://hive.apache.org) and Hadoop [Map/Reduce](http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html). Works with any OpenSearch cluster accessible via REST, including Amazon OpenSearch Service and Amazon OpenSearch Serverless.

**Use cases:**
- Index large datasets from Spark ETL pipelines into OpenSearch
- Query OpenSearch from Spark for analytics, reporting, and machine learning
- Build search-powered applications backed by Spark data pipelines
- Bridge your data lake or lakehouse with search and analytics on OpenSearch

## Quick Start

Write a Spark DataFrame to OpenSearch and read it back, using PySpark:

```bash
pyspark --packages org.opensearch.client:opensearch-spark-30_2.12:2.0.0 \
        --conf spark.opensearch.nodes=localhost \
        --conf spark.opensearch.nodes.wan.only=true
```

```python
# Write
df = spark.createDataFrame([("hello", 1), ("world", 2)], ["name", "value"])
df.write.format("opensearch").save("my-index")

# Read
result = spark.read.format("opensearch").load("my-index")
result.show()
```

For Scala, Java, Spark SQL, RDD, and more examples, see the [User Guide](USER_GUIDE.md).
For Amazon OpenSearch Service and OpenSearch Serverless, see the [User Guide](USER_GUIDE.md#amazon-opensearch-service).

## Maven Coordinates

Choose the artifact that matches your Spark and Scala version:

| Spark Version | Scala Version | Artifact |
|---------------|---------------|----------|
| 3.4.x | 2.12 | `org.opensearch.client:opensearch-spark-30_2.12:2.0.0` |
| 3.4.x | 2.13 | `org.opensearch.client:opensearch-spark-30_2.13:2.0.0` |
| 3.5.x | 2.12 | `org.opensearch.client:opensearch-spark-35_2.12:2.0.0` |
| 3.5.x | 2.13 | `org.opensearch.client:opensearch-spark-35_2.13:2.0.0` |
| 4.x | 2.13 | `org.opensearch.client:opensearch-spark-40_2.13:2.0.0` |

For Map/Reduce and Hive, see `org.opensearch.client:opensearch-hadoop-mr` and `org.opensearch.client:opensearch-hadoop-hive` on [Maven Central](https://central.sonatype.com/search?q=org.opensearch.client%20opensearch-hadoop).

## Requirements

- OpenSearch 1.x or later (including Amazon OpenSearch Service and Serverless)
- Java 11 or later at runtime
- Java 21 to build from source
- For SigV4 IAM authentication, additional AWS SDK dependencies are required. See the [User Guide](USER_GUIDE.md).

## Compatibility

See [COMPATIBILITY.md](COMPATIBILITY.md).

## Building from Source

OpenSearch Hadoop uses [Gradle](http://www.gradle.org/) for its build system. JDK 21 is required.

```bash
./gradlew build           # build and run unit tests
./gradlew integrationTests # run integration tests
./gradlew distZip          # create distributable zip
```

## Documentation

- [User Guide](USER_GUIDE.md) — usage examples for Spark, Hive, and Map/Reduce
- [Compatibility](COMPATIBILITY.md) — supported versions of OpenSearch, Spark, and Scala
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## License

This project is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
