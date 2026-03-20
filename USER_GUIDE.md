# User Guide

- [Spark](#spark)
  - [Setup](#setup)
  - [PySpark](#pyspark)
  - [Scala](#scala)
  - [Java](#java)
  - [Spark SQL](#spark-sql)
  - [Spark RDD](#spark-rdd)
  - [Structured Streaming](#structured-streaming)
  - [Common Patterns](#common-patterns)
- [Configuration Properties](#configuration-properties)
  - [Required](#required)
  - [Essential](#essential)
- [Amazon OpenSearch Service](#amazon-opensearch-service)
- [Amazon OpenSearch Serverless](#amazon-opensearch-serverless)
- [Map/Reduce](#mapreduce)
  - [Old API (`org.apache.hadoop.mapred`)](#old-api-orgapachehadoopmapred)
  - [New API (`org.apache.hadoop.mapreduce`)](#new-api-orgapachehadoopmapreduce)
- [Apache Hive](#apache-hive)

## Spark

### Setup

Add the opensearch-hadoop connector to your Spark application using `--packages`:

```bash
# Spark 3.4.x
pyspark --packages org.opensearch.client:opensearch-spark-30_2.12:2.0.0

# Spark 3.5.x
pyspark --packages org.opensearch.client:opensearch-spark-35_2.12:2.0.0

# Spark 4.x
pyspark --packages org.opensearch.client:opensearch-spark-40_2.13:2.0.0
```

Or add it as a dependency in your build file:

```xml
<!-- Maven (Spark 3.4.x, Scala 2.12) -->
<dependency>
    <groupId>org.opensearch.client</groupId>
    <artifactId>opensearch-spark-30_2.12</artifactId>
    <version>2.0.0</version>
</dependency>
```

```groovy
// Gradle (Spark 3.4.x, Scala 2.12)
implementation 'org.opensearch.client:opensearch-spark-30_2.12:2.0.0'
```

See the [README](README.md#maven-coordinates) for the full list of artifacts for each Spark and Scala version.

### PySpark

No additional Python package is needed. The Java connector is loaded via `--packages` or `spark.jars`.

```python
# Write (index documents into OpenSearch)
df = spark.createDataFrame([("John", 30), ("Jane", 25)], ["name", "age"])
df.write.format("opensearch").save("people")

# Read (query documents from OpenSearch)
df = spark.read.format("opensearch").load("people")
df.show()

# Read with a query (only matching documents are transferred to Spark)
filtered = spark.read \
    .format("opensearch") \
    .option("opensearch.query", '{"query":{"match":{"name":"John"}}}') \
    .load("people")
```

### Scala

```scala
import org.opensearch.spark.sql._

// Write (index documents into OpenSearch)
val df = spark.createDataFrame(Seq(("John", 30), ("Jane", 25))).toDF("name", "age")
df.saveToOpenSearch("people")

// With options
df.saveToOpenSearch("people", Map(
  "opensearch.nodes" -> "my-cluster",
  "opensearch.port" -> "9200"
))

// Read (query documents from OpenSearch)
val result = spark.read.format("opensearch").load("people")
result.show()

// Read with a query
val filtered = spark.read
  .format("opensearch")
  .option("opensearch.query", """{"query":{"match":{"name":"John"}}}""")
  .load("people")
```

### Java

```java
import org.opensearch.spark.sql.api.java.JavaOpenSearchSparkSQL;

// Write
Dataset<Row> df = spark.createDataFrame(data, schema);
JavaOpenSearchSparkSQL.saveToOpenSearch(df, "people");

// Read
Dataset<Row> result = spark.read().format("opensearch").load("people");
result.show();
```

### Spark SQL

You can register an OpenSearch index as a temporary view and query it with SQL:

```python
spark.sql("""
  CREATE TEMPORARY VIEW people
  USING opensearch
  OPTIONS (resource 'people')
""")

spark.sql("SELECT * FROM people WHERE age > 25").show()
```

### Spark RDD

For low-level access, opensearch-hadoop provides RDD-based read and write methods.

#### Scala

```scala
import org.opensearch.spark._

// Write
val data = sc.makeRDD(Seq(
  Map("name" -> "John", "age" -> 30),
  Map("name" -> "Jane", "age" -> 25)
))
data.saveToOpenSearch("people")

// Read
val rdd = sc.opensearchRDD("people")
rdd.collect().foreach(println)

// Read with query
val filtered = sc.opensearchRDD("people", "?q=name:John")
```

#### Java

```java
import org.opensearch.spark.rdd.api.java.JavaOpenSearchSpark;

// Write
JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(data);
JavaOpenSearchSpark.saveToOpenSearch(javaRDD, "people");

// Read
JavaPairRDD<String, Map<String, Object>> rdd = JavaOpenSearchSpark.opensearchRDD(jsc, "people");
```

### Structured Streaming

opensearch-hadoop supports Spark Structured Streaming as a sink:

```scala
val query = streamingDF.writeStream
  .format("opensearch")
  .option("checkpointLocation", "/tmp/checkpoint")
  .start("streaming-index")
```

### Common Patterns

#### Specifying Document ID

Use `opensearch.mapping.id` to control the `_id` of each document. This is useful for upserts and deduplication:

```python
df.write.format("opensearch") \
    .option("opensearch.mapping.id", "id") \
    .save("my-index")
```

#### Write Modes

Spark's `SaveMode` controls how data is written:

```python
# Append (default): add documents to the index
df.write.format("opensearch").mode("append").save("my-index")

# Overwrite: delete the index and recreate it with the new data
df.write.format("opensearch").mode("overwrite").save("my-index")
```

#### Upsert

Update existing documents or insert new ones using `opensearch.write.operation`:

```python
df.write.format("opensearch") \
    .option("opensearch.mapping.id", "id") \
    .option("opensearch.write.operation", "upsert") \
    .save("my-index")
```

Other write operations: `index` (default), `create`, `update`.

#### Reading with a Query

Filter data at the OpenSearch level using `opensearch.query`, so only matching documents are loaded into Spark:

```python
# Query DSL
df = spark.read.format("opensearch") \
    .option("opensearch.query", '{"query":{"range":{"age":{"gte":25}}}}') \
    .load("my-index")

# URI query
df = spark.read.format("opensearch") \
    .option("opensearch.query", "?q=name:John") \
    .load("my-index")
```

#### Selecting Fields

Load only specific fields from OpenSearch to reduce data transfer:

```python
df = spark.read.format("opensearch") \
    .option("opensearch.read.field.include", "name,age") \
    .load("my-index")
```

#### Scroll Size

Control how many documents are fetched per batch when reading:

```python
df = spark.read.format("opensearch") \
    .option("opensearch.scroll.size", "5000") \
    .load("my-index")
```

#### Basic Authentication

```python
df.write.format("opensearch") \
    .option("opensearch.net.http.auth.user", "<username>") \
    .option("opensearch.net.http.auth.pass", "<password>") \
    .save("my-index")
```

#### HTTPS

```python
df.write.format("opensearch") \
    .option("opensearch.net.ssl", "true") \
    .save("my-index")
```

#### Separate Read and Write Indices

Use different indices for reading and writing:

```python
# Write to a specific index
df.write.format("opensearch") \
    .option("opensearch.resource.write", "logs-2026.03") \
    .save("logs-2026.03")

# Read from an alias or different index
df = spark.read.format("opensearch") \
    .option("opensearch.resource.read", "logs-alias") \
    .load("logs-alias")
```

#### Dynamic Index Routing

Use placeholders in the index name to route documents to different indices based on field values. This feature requires the Scala `saveToOpenSearch` method:

```scala
import org.opensearch.spark.sql._

// Route by field value: {"category": "electronics", "name": "TV"} -> index "electronics"
df.saveToOpenSearch("{category}")

// Prefix + field value: {"env": "prod", "msg": "ok"} -> index "logs-prod"
df.saveToOpenSearch("logs-{env}")

// Date formatting: {"timestamp": "2026-02-16T10:30:00.000Z", "msg": "ok"} -> index "logs-2026.02.16"
df.saveToOpenSearch("logs-{timestamp|yyyy.MM.dd}")
```

## Configuration Properties

All configuration properties start with the `opensearch` prefix. The `opensearch.internal` namespace is reserved for internal use.

Properties can be set via Spark configuration (`--conf`), as options on the DataFrame reader/writer, or in the Hadoop configuration.

### Required

| Property | Description |
|----------|-------------|
| `opensearch.resource` | OpenSearch index name (e.g., `my-index`). Can also be specified as the argument to `saveToOpenSearch()` or `load()`. |

### Essential

| Property | Default | Description |
|----------|---------|-------------|
| `opensearch.nodes` | `localhost` | OpenSearch host address |
| `opensearch.port` | `9200` | OpenSearch REST port |
| `opensearch.nodes.wan.only` | `false` | Set to `true` when connecting through a load balancer or proxy (e.g., Docker, Kubernetes, cloud environments) |
| `opensearch.query` | match all | Query DSL or URI query for reading (e.g., `{"query":{"match":{"name":"John"}}}`) |
| `opensearch.net.ssl` | `false` | Enable HTTPS |
| `opensearch.mapping.id` | (none) | Document field to use as the `_id` |
| `opensearch.write.operation` | `index` | Write operation: `index`, `create`, `update`, `upsert` |

## Amazon OpenSearch Service

To connect to Amazon OpenSearch Service with IAM authentication, enable SigV4 signing and HTTPS:

```python
df.write.format("opensearch") \
    .option("opensearch.nodes", "https://search-xxx.us-east-1.es.amazonaws.com") \
    .option("opensearch.port", "443") \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.aws.sigv4.enabled", "true") \
    .option("opensearch.aws.sigv4.region", "us-east-1") \
    .save("my-index")
```

Reading works the same way:

```python
df = spark.read.format("opensearch") \
    .option("opensearch.nodes", "https://search-xxx.us-east-1.es.amazonaws.com") \
    .option("opensearch.port", "443") \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.aws.sigv4.enabled", "true") \
    .option("opensearch.aws.sigv4.region", "us-east-1") \
    .load("my-index")
```

The following AWS SDK v2 dependencies are required on the classpath:
- `software.amazon.awssdk:auth:2.31.59` (or later)
- `software.amazon.awssdk:regions:2.31.59` (or later)
- `software.amazon.awssdk:http-client-spi:2.31.59` (or later)
- `software.amazon.awssdk:identity-spi:2.31.59` (or later)
- `software.amazon.awssdk:sdk-core:2.31.59` (or later)
- `software.amazon.awssdk:utils:2.31.59` (or later)

## Amazon OpenSearch Serverless

To connect to Amazon OpenSearch Serverless, add `opensearch.serverless` and set the SigV4 service name to `aoss`:

```python
df.write.format("opensearch") \
    .option("opensearch.nodes", "https://xxx.us-east-1.aoss.amazonaws.com") \
    .option("opensearch.port", "443") \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.aws.sigv4.enabled", "true") \
    .option("opensearch.aws.sigv4.region", "us-east-1") \
    .option("opensearch.aws.sigv4.service.name", "aoss") \
    .option("opensearch.serverless", "true") \
    .save("my-index")
```

You can configure the page size for reads with `opensearch.search_after.size` (default: 1000):

```python
df = spark.read.format("opensearch") \
    .option("opensearch.nodes", "https://xxx.us-east-1.aoss.amazonaws.com") \
    .option("opensearch.port", "443") \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.aws.sigv4.enabled", "true") \
    .option("opensearch.aws.sigv4.region", "us-east-1") \
    .option("opensearch.aws.sigv4.service.name", "aoss") \
    .option("opensearch.serverless", "true") \
    .option("opensearch.search_after.size", "1000") \
    .load("my-index")
```

## Map/Reduce

For low-level Hadoop Map/Reduce jobs, opensearch-hadoop provides `OpenSearchInputFormat` and `OpenSearchOutputFormat`. Add `opensearch-hadoop-mr-2.0.0.jar` to your job classpath.

### Old API (`org.apache.hadoop.mapred`)

```java
// Reading
JobConf conf = new JobConf();
conf.setInputFormat(OpenSearchInputFormat.class);
conf.set("opensearch.resource", "my-index");
conf.set("opensearch.query", "?q=name:John");
JobClient.runJob(conf);

// Writing
JobConf conf = new JobConf();
conf.setOutputFormat(OpenSearchOutputFormat.class);
conf.set("opensearch.resource", "my-index");
JobClient.runJob(conf);
```

### New API (`org.apache.hadoop.mapreduce`)

```java
// Reading
Configuration conf = new Configuration();
conf.set("opensearch.resource", "my-index");
conf.set("opensearch.query", "?q=name:John");
Job job = new Job(conf);
job.setInputFormatClass(OpenSearchInputFormat.class);
job.waitForCompletion(true);

// Writing
Configuration conf = new Configuration();
conf.set("opensearch.resource", "my-index");
Job job = new Job(conf);
job.setOutputFormatClass(OpenSearchOutputFormat.class);
job.waitForCompletion(true);
```

## Apache Hive

opensearch-hadoop provides a Hive storage handler. Add `opensearch-hadoop-hive-2.0.0.jar` to your Hive classpath:

```sql
ADD JAR /path/opensearch-hadoop-hive-2.0.0.jar;
```

```sql
-- Create an external table backed by an OpenSearch index
CREATE EXTERNAL TABLE people (
    name STRING,
    age  INT)
STORED BY 'org.opensearch.hadoop.hive.OpenSearchStorageHandler'
TBLPROPERTIES('opensearch.resource' = 'people');

-- Read
SELECT * FROM people;

-- Write from another table
INSERT OVERWRITE TABLE people SELECT name, age FROM source;
```

For IAM authentication with Hive, add the SigV4 properties to `TBLPROPERTIES`:

```sql
CREATE EXTERNAL TABLE people (
    name STRING,
    age  INT)
STORED BY 'org.opensearch.hadoop.hive.OpenSearchStorageHandler'
TBLPROPERTIES(
    'opensearch.nodes' = 'https://search-xxx.us-east-1.es.amazonaws.com',
    'opensearch.port' = '443',
    'opensearch.net.ssl' = 'true',
    'opensearch.resource' = 'people',
    'opensearch.nodes.wan.only' = 'true',
    'opensearch.aws.sigv4.enabled' = 'true',
    'opensearch.aws.sigv4.region' = 'us-east-1');
```
