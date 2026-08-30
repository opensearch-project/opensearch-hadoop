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
- [Google Managed Service for Apache Spark](#google-managed-service-for-apache-spark)
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

### Parallel Reads

Serverless mode automatically splits reads into parallel partitions using PIT + Slice. By default, the connector creates one partition per 50,000 documents in each index.

To customize the partition size, set `opensearch.input.max.docs.per.partition`:

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
    .option("opensearch.input.max.docs.per.partition", "100000") \
    .load("my-index")
```

The connector will count the documents in each index, divide by this value to determine the number of slices, and create one Spark partition per slice. For example, an index with 1,000,000 documents and `max.docs.per.partition=100000` will produce 10 parallel read tasks.

## Google Managed Service for Apache Spark

Google Managed Service for Apache Spark (previously Dataproc Serverless, also documented as Google Cloud Serverless for Apache Spark) runs stock Apache Spark, so the connector works there using the standard artifacts. Pick the artifact that matches your runtime version:

| Runtime version | Apache Spark | Scala | Artifact |
|-----------------|--------------|-------|----------|
| 1.2 LTS | 3.5.x | 2.13 | `org.opensearch.client:opensearch-spark-35_2.13` |
| 2.2 LTS | 3.5.x | 2.13 | `org.opensearch.client:opensearch-spark-35_2.13` |
| 2.3 LTS | 3.5.x | 2.13 | `org.opensearch.client:opensearch-spark-35_2.13` |
| 3.0 | 4.0.x | 2.13 | `org.opensearch.client:opensearch-spark-40_2.13` |

### Providing the connector jar

Submit the connector with `spark.jars.packages` so that Maven resolves it and its runtime dependencies:

```bash
gcloud dataproc batches submit pyspark my_job.py \
    --region=us-central1 \
    --version=2.2 \
    --properties=\
spark.jars.packages=org.opensearch.client:opensearch-spark-35_2.13:2.0.0,\
spark.opensearch.nodes=https://opensearch.example.com,\
spark.opensearch.port=443,\
spark.opensearch.net.ssl=true,\
spark.opensearch.nodes.wan.only=true
```

Spark options may be passed as `spark.opensearch.*`; the connector strips the `spark.` prefix.

Alternatively, stage a jar on Cloud Storage and reference it with `--jars`. Note that the connector jar published to Maven Central is not an uber jar: it deliberately excludes third-party runtime dependencies such as the Google Auth Library so that they can be resolved and upgraded independently. If you supply the jar with `--jars` instead of `spark.jars.packages`, you are responsible for also supplying those dependencies, either by listing them alongside your jar or by building an uber jar yourself. This is the same tradeoff that applies to the AWS SDK dependencies required for SigV4.

### Authenticating with Google credentials

For clusters that trust Google as an identity provider, the connector can authenticate using the ambient GCP identity of the Spark workload rather than a static username and password. Credentials are resolved from Application Default Credentials, which in Managed Service for Apache Spark means the service account attached to the batch or session.

Two flows are available, selected with `opensearch.gcp.oidc.token.type`.

**OIDC ID token (default).** The connector mints a Google-signed ID token for a target audience and sends it as a bearer token. Configure the OpenSearch security plugin's OpenID Connect or JWT authentication backend to trust Google as the issuer, then map the token's `sub` claim to a role.

Use `sub` rather than `email`. Google only includes an `email` claim when the token is requested from the Compute Engine metadata server with the `FORMAT_FULL` option; tokens minted from a service account key file never carry one. `sub`, a numeric subject identifier, is present in both cases.

Read the `sub` value out of a real token rather than assuming it. It is usually the service account's IAM unique ID, but not always: on Managed Service for Apache Spark runtime 3.0 the tokens issued to a workload carry a subject that differs from the unique ID of the attached service account, even though the metadata server reports that account's email. To collect the value, request a token for your audience from inside the workload and decode its payload:

```python
import base64, json, urllib.request

url = ("http://metadata.google.internal/computeMetadata/v1/instance/"
       "service-accounts/default/identity?audience=https://opensearch.example.com")
token = urllib.request.urlopen(
    urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})).read().decode()
payload = token.split(".")[1]
print(json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))))
```

If the same job runs on more than one runtime version, map every `sub` you observe. An unmapped subject authenticates successfully and then fails authorization, so the symptom is `403 Forbidden`, not `401 Unauthorized`.

```python
df.write.format("opensearch") \
    .option("opensearch.nodes", "https://opensearch.example.com") \
    .option("opensearch.port", "443") \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.gcp.oidc.enabled", "true") \
    .option("opensearch.gcp.oidc.audience", "https://opensearch.example.com") \
    .save("my-index")
```

A matching `jwt` authentication domain in the security plugin's `config.yml` looks like this. The audience must be identical to `opensearch.gcp.oidc.audience`:

```yaml
jwt_auth_domain:
  http_enabled: true
  order: 0
  http_authenticator:
    type: jwt
    challenge: false
    config:
      jwks_uri: 'https://www.googleapis.com/oauth2/v3/certs'
      required_issuer: 'https://accounts.google.com'
      required_audience: 'https://opensearch.example.com'
      subject_key: sub
  authentication_backend:
    type: noop
```

Because the authentication backend is `noop`, the token carries no backend roles, so map the `sub` value to a role directly in `roles_mapping.yml`.

The role itself needs more than the built-in `read` action group, which does not include the index existence check, the shard routing lookup, or scroll clearing that the connector performs. This is a property of the connector's request pattern and applies equally to basic authentication:

```yaml
opensearch_hadoop_reader:
  cluster_permissions:
    - cluster_composite_ops_ro
    - cluster_monitor
  index_permissions:
    - index_patterns: ["*"]
      allowed_actions:
        - read
        - "indices:admin/exists"
        - "indices:admin/get"
        - "indices:admin/mappings/get"
        - "indices:admin/shards/search_shards"
        - "indices:data/read/scroll"
        - "indices:data/read/scroll/clear"
        - "indices:data/read/point_in_time/create"
        - "indices:data/read/point_in_time/delete"
        - "indices:data/read/search/point_in_time"
```

Writing needs more than the `write` action group for the same reason. A role that can create the index, put its mapping, index documents and refresh looks like this:

```yaml
opensearch_hadoop_writer:
  cluster_permissions:
    - cluster_composite_ops
    - cluster_monitor
    - "indices:data/read/scroll"
    - "indices:data/read/scroll/clear"
  index_permissions:
    - index_patterns: ["my-index*"]
      allowed_actions:
        - read
        - write
        - create_index
        - "indices:admin/exists"
        - "indices:admin/get"
        - "indices:admin/mappings/get"
        - "indices:admin/mapping/put"
        - "indices:admin/shards/search_shards"
        - "indices:admin/refresh*"
        - "indices:data/read/scroll"
        - "indices:data/read/scroll/clear"
```

Note the wildcard on `indices:admin/refresh*`. The connector refreshes the index once the write finishes, and OpenSearch authorizes that as the shard level action `indices:admin/refresh[s]`, which a bare `indices:admin/refresh` grant does not cover.

Get this wrong and the failure is easy to misread: the bulk requests are authorized and the documents land, then the trailing refresh is denied and the Spark job aborts. The index is left holding data from a job that reported failure, so check the document count before assuming the write did not happen.

**OAuth2 access token.** For deployments that place an authenticating proxy in front of OpenSearch (for example Identity-Aware Proxy), request an access token for one or more scopes instead:

```python
df = spark.read.format("opensearch") \
    .option("opensearch.nodes", "https://opensearch.example.com") \
    .option("opensearch.port", "443") \
    .option("opensearch.net.ssl", "true") \
    .option("opensearch.nodes.wan.only", "true") \
    .option("opensearch.gcp.oidc.enabled", "true") \
    .option("opensearch.gcp.oidc.token.type", "access_token") \
    .option("opensearch.gcp.oidc.scopes", "https://www.googleapis.com/auth/cloud-platform") \
    .load("my-index")
```

Google tokens are short lived, typically one hour. The connector resolves a token per request and caches it until it approaches expiry, so batch and structured streaming jobs that run longer than the token lifetime continue to work without reconfiguration.

| Property | Default | Description |
|----------|---------|-------------|
| `opensearch.gcp.oidc.enabled` | `false` | Enable authentication using Google credentials resolved from Application Default Credentials |
| `opensearch.gcp.oidc.token.type` | `id_token` | Either `id_token` or `access_token` |
| `opensearch.gcp.oidc.audience` | (none) | Target audience for the ID token. Required when the token type is `id_token` |
| `opensearch.gcp.oidc.scopes` | `https://www.googleapis.com/auth/cloud-platform` | Comma separated OAuth2 scopes. Used when the token type is `access_token` |
| `opensearch.gcp.oidc.token.refresh.window` | `300` | Seconds before expiry at which a cached token is refreshed |

The following dependency is required on the classpath and is resolved automatically when the connector is added with `spark.jars.packages`:
- `com.google.auth:google-auth-library-oauth2-http:1.30.1` (or later)

Google authentication and AWS SigV4 signing are independent. Enable only the one that matches your target cluster.

### Verifying a deployment

To validate the connector on a runtime manually, submit a batch that writes a small DataFrame and reads it back:

```python
# gs://your-bucket/verify_opensearch.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify-opensearch").getOrCreate()

opts = {
    "opensearch.nodes": "https://opensearch.example.com",
    "opensearch.port": "443",
    "opensearch.net.ssl": "true",
    "opensearch.nodes.wan.only": "true",
    "opensearch.gcp.oidc.enabled": "true",
    "opensearch.gcp.oidc.audience": "https://opensearch.example.com",
}

spark.createDataFrame([("hello", 1), ("world", 2)], ["name", "value"]) \
    .write.format("opensearch").options(**opts).mode("overwrite").save("gms-verify")

count = spark.read.format("opensearch").options(**opts).load("gms-verify").count()
assert count == 2, "expected 2 documents, found %d" % count
print("verified: read back %d documents" % count)
```

```bash
gcloud dataproc batches submit pyspark gs://your-bucket/verify_opensearch.py \
    --region=us-central1 \
    --version=2.2 \
    --properties=spark.jars.packages=org.opensearch.client:opensearch-spark-35_2.13:2.0.0
```

A successful run writes two documents and reads the same count back.

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

