- [Compatibility with Java](#compatibility-with-java)
- [Compatibility with OpenSearch](#compatibility-with-opensearch)
- [Compatibility with Spark and Scala](#compatibility-with-spark-and-scala)
- [Compatibility with AWS Glue](#compatibility-with-aws-glue)

## Compatibility with Java

| Client Version | Minimum Runtime | Build Requirement |
|----------------|-----------------|-------------------|
| 1.0.0-1.3.0   | Java 8          | JDK 14            |
| 2.0.0          | Java 11         | JDK 21            |

## Compatibility with OpenSearch

The below matrix shows the compatibility of the [`opensearch-hadoop`](https://central.sonatype.com/artifact/org.opensearch.client/opensearch-hadoop) with versions of [`OpenSearch`](https://opensearch.org/downloads.html#opensearch).

| Client Version | OpenSearch Version |
|----------------|--------------------|
| 1.0.0-1.3.0   | 1.x, 2.x          |
| 2.0.0          | 1.x, 2.x, 3.x     |

## Compatibility with Spark and Scala

| Client Version | Module | Spark Version | Scala Version(s) |
|----------------|--------|---------------|-------------------|
| 1.0.0-1.3.0   | opensearch-spark-30 | 3.4.x | 2.12, 2.13 |
| 2.0.0          | opensearch-spark-30 | 3.4.x | 2.12, 2.13 |
| 2.0.0          | opensearch-spark-35 | 3.5.x | 2.12, 2.13 |
| 2.0.0          | opensearch-spark-40 | 4.x   | 2.13       |

Note: Client versions 1.0.0-1.3.0 can be used with Spark 3.5.x for batch reads and writes (Spark SQL, RDD). Structured Streaming writes require version 2.0.0 with the dedicated `opensearch-spark-35` module.

## Compatibility with AWS Glue

| Client Version | Spark Version | Glue Version(s) |
|----------------|---------------|-----------------|
| 1.0.0-1.3.0   | 3.4.x         | 3, 4            |
| 2.0.0          | 3.5.x         | 5.0, 5.1        |
