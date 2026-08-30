- [Compatibility with Java](#compatibility-with-java)
- [Compatibility with OpenSearch](#compatibility-with-opensearch)
- [Compatibility with Spark and Scala](#compatibility-with-spark-and-scala)
- [Compatibility with AWS Glue](#compatibility-with-aws-glue)
- [Compatibility with Google Managed Service for Apache Spark](#compatibility-with-google-managed-service-for-apache-spark)

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

## Compatibility with Google Managed Service for Apache Spark

Google Managed Service for Apache Spark (previously Dataproc Serverless) runs stock Apache Spark, so the standard modules apply.

| Client Version | Module | Runtime Version | Spark Version | Scala Version |
|----------------|--------|-----------------|---------------|---------------|
| 2.0.0          | opensearch-spark-35 | 1.2 LTS | 3.5.1 | 2.12 |
| 2.0.0          | opensearch-spark-35 | 2.2 LTS | 3.5.3 | 2.13 |
| 2.0.0          | opensearch-spark-35 | 2.3     | 3.5.3 | 2.13 |
| 2.0.0          | opensearch-spark-40 | 3.0     | 4.0.1 | 2.13 |

Match the artifact's Scala version to the runtime's: runtime 1.2 LTS needs the Scala 2.12 build of `opensearch-spark-35`, and the later runtimes need the Scala 2.13 build. Mixing them fails at class-load time rather than at submission.

Authenticating to OpenSearch with the workload's Google identity is supported from client version 2.0.0; see the [User Guide](USER_GUIDE.md#authenticating-with-google-credentials). Note that the `sub` claim in the ID token issued to a workload is not the same on every runtime version, so read it from a real token before writing the role mapping.
