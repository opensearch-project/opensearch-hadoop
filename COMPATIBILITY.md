- [Compatibility with OpenSearch](#compatibility-with-opensearch)
- [Compatibility with Spark and Scala](#compatibility-with-spark-and-scala)
- [Compatibility with AWS Glue](#compatibility-with-aws-glue)

## Compatibility with OpenSearch

The below matrix shows the compatibility of the [`opensearch-hadoop`](https://central.sonatype.com/artifact/org.opensearch.client/opensearch-hadoop) with versions of [`OpenSearch`](https://opensearch.org/downloads.html#opensearch).

| Client Version | OpenSearch Version | ElasticSearch Version |
|----------------|--------------------| --------------------- |
| 1.0.0-1.1.0    | 1.0.0-2.12.0       | 7.x                   |

## Compatibility with Spark and Scala

| Client Version | Spark Version | Scala Version(s) |
|----------------| ------------- | ---------------- |
| 1.0.0-1.3.0    | 3.4.4         | 2.12/2.13        |
| unreleased     | 3.5.x         | 2.12/2.13        |
| unreleased     | 4.1.1         | 2.13             |

Note: Client versions 1.0.0-1.3.0 can be used with Spark 3.5.x for batch reads and writes (Spark SQL, RDD). Structured Streaming writes require the upcoming release with the dedicated `opensearch-spark-35` module.

## Compatibility with AWS Glue

| Client Version | Spark Version | Glue Version(s) |
|----------------| ------------- | --------------- |
| 1.0.0-1.3.0    | 3.4.4         | 3/4             |
| unreleased     | 3.5.x         | 5.0/5.1         |
