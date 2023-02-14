[![Build & Test MapReduce](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-mr.yml/badge.svg?branch=main)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-mr.yml)
[![Build & Test Spark](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-spark.yml/badge.svg?branch=main)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-spark.yml)
[![Build & Test Hive](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-hive.yml/badge.svg?branch=main)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-hive.yml)
[![Build & Test Pig](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-pig.yml/badge.svg?branch=main)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-pig.yml)
[![Build & Test Storm](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-storm.yml/badge.svg?branch=main)](https://github.com/opensearch-project/opensearch-hadoop/actions/workflows/build-storm.yml)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)
![OpenSearch logo](OpenSearch.svg)

# OpenSearch Hadoop
OpenSearch real-time search and analytics natively integrated with Hadoop.
Supports [Map/Reduce](#mapreduce), [Apache Hive](#apache-hive), [Apache Pig](#apache-pig), [Apache Spark](#apache-spark) and [Apache Storm](#apache-storm).

- [OpenSearch Hadoop](#opensearch-hadoop)
  - [Requirements](#requirements)
  - [Usage](#usage)
    - [Configuration Properties](#configuration-properties)
    - [Required](#required)
    - [Essential](#essential)
  - [Map/Reduce](#mapreduce)
    - ['Old' (`org.apache.hadoop.mapred`) API](#old-orgapachehadoopmapred-api)
    - [Reading](#reading)
    - [Writing](#writing)
    - ['New' (`org.apache.hadoop.mapreduce`) API](#new-orgapachehadoopmapreduce-api)
    - [Reading](#reading-1)
    - [Writing](#writing-1)
    - [Signing requests for IAM authentication](#signing-requests-for-iam-authentication)
  - [Apache Hive](#apache-hive)
    - [Reading](#reading-2)
    - [Writing](#writing-2)
    - [Signing requests for IAM authentication](#signing-requests-for-iam-authentication-1)
  - [Apache Pig](#apache-pig)
    - [Reading](#reading-3)
    - [Writing](#writing-3)
  - [Apache Spark](#apache-spark)
    - [Scala](#scala)
    - [Reading](#reading-4)
      - [Spark SQL](#spark-sql)
    - [Writing](#writing-4)
      - [Spark SQL](#spark-sql-1)
    - [Java](#java)
    - [Reading](#reading-5)
      - [Spark SQL](#spark-sql-2)
    - [Writing](#writing-5)
      - [Spark SQL](#spark-sql-3)
    - [Signing requests for IAM authentication](#signing-requests-for-iam-authentication-2)
  - [Apache Storm](#apache-storm)
    - [Reading](#reading-6)
    - [Writing](#writing-6)
  - [Building the source](#building-the-source)
  - [License](#license)

## Requirements
OpenSearch (__3.x__ or higher) cluster accessible through [REST][]. That's it!
If using SigV4 IAM auth features, you would need to include the [aws-sdk-bundle](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle) in your job classpath.

## Usage

### Configuration Properties

All configuration properties start with `opensearch` prefix. Note that the `opensearch.internal` namespace is reserved for the library internal use and should _not_ be used by the user at any point.
The properties are read mainly from the Hadoop configuration but the user can specify (some of) them directly depending on the library used.

### Required
```
opensearch.resource=<OpenSearch resource location, relative to the host/port specified above>
```
### Essential
```
opensearch.query=<uri or query dsl query>      # defaults to {"query":{"match_all":{}}}
opensearch.nodes=<OpenSearch host address>     # defaults to localhost
opensearch.port=<OPENSEARCH REST port>         # defaults to 9200
```

## [Map/Reduce][]

For basic, low-level or performance-sensitive environments, OpenSearch-Hadoop provides dedicated `InputFormat` and `OutputFormat` that read and write data to OpenSearch. To use them, add the `opensearch-hadoop` jar to your job classpath
(either by bundling the library along - it's ~300kB and there are no-dependencies), using the [DistributedCache][] or by provisioning the cluster manually.

Note that os-hadoop supports both the so-called 'old' and the 'new' API through its `OpenSearchInputFormat` and `OpenSearchOutputFormat` classes.

### 'Old' (`org.apache.hadoop.mapred`) API

### Reading
To read data from OpenSearch, configure the `OpenSearchInputFormat` on your job configuration along with the relevant [properties](#configuration-properties):
```java
JobConf conf = new JobConf();
conf.setInputFormat(OpenSearchInputFormat.class);
conf.set("opensearch.resource", "radio/artists");
conf.set("opensearch.query", "?q=me*");             // replace this with the relevant query
...
JobClient.runJob(conf);
```
### Writing
Same configuration template can be used for writing but using `OpenSearchOuputFormat`:
```java
JobConf conf = new JobConf();
conf.setOutputFormat(OpenSearchOutputFormat.class);
conf.set("opensearch.resource", "radio/artists"); // index or indices used for storing data
...
JobClient.runJob(conf);
```
### 'New' (`org.apache.hadoop.mapreduce`) API

### Reading
```java
Configuration conf = new Configuration();
conf.set("opensearch.resource", "radio/artists");
conf.set("opensearch.query", "?q=me*");             // replace this with the relevant query
Job job = new Job(conf)
job.setInputFormatClass(OpenSearchInputFormat.class);
...
job.waitForCompletion(true);
```
### Writing
```java
Configuration conf = new Configuration();
conf.set("opensearch.resource", "radio/artists"); // index or indices used for storing data
Job job = new Job(conf)
job.setOutputFormatClass(OpenSearchOutputFormat.class);
...
job.waitForCompletion(true);
```

### Signing requests for IAM authentication

Signing requests would require the [aws-sdk-bundle](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle) in your job classpath.

```java
Configuration conf = new Configuration();
conf.set("opensearch.resource", "radio/artists");
conf.set("opensearch.query", "?q=me*");             // replace this with the relevant query
conf.set("opensearch.nodes", "https://search-xxx.us-east-1.es.amazonaws.com");
conf.set("opensearch.port", "443");
conf.set("opensearch.net.ssl", "true");
conf.set("opensearch.nodes.wan.only", "true");
conf.set("opensearch.aws.sigv4.enabled", "true");
conf.set("opensearch.aws.sigv4.region", "us-east-1");
Job job = new Job(conf)
job.setInputFormatClass(OpenSearchInputFormat.class);
...
job.waitForCompletion(true);
```

## [Apache Hive][]
OpenSearch-Hadoop provides a Hive storage handler for OpenSearch, meaning one can define an [external table][] on top of OpenSearch.

Add opensearch-hadoop-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/opensearch-hadoop-<version>.jar;
```
### Reading
To read data from OpenSearch, define a table backed by the desired index:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.opensearch.hive.hadoop.OpenSearchStorageHandler'
TBLPROPERTIES('opensearch.resource' = 'radio/artists', 'opensearch.query' = '?q=me*');
```
The fields defined in the table are mapped to the JSON when communicating with OpenSearch. Notice the use of `TBLPROPERTIES` to define the location, that is the query used for reading from this table.

Once defined, the table can be used just like any other:
```SQL
SELECT * FROM artists;
```

### Writing
To write data, a similar definition is used but with a different `opensearch.resource`:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.opensearch.hive.hadoop.OpenSearchStorageHandler'
TBLPROPERTIES('opensearch.resource' = 'radio/artists');
```

Any data passed to the table is then passed down to OpenSearch; for example considering a table `s`, mapped to a TSV/CSV file, one can index it to OpenSearch like this:
```SQL
INSERT OVERWRITE TABLE artists
    SELECT NULL, s.name, named_struct('url', s.url, 'picture', s.picture) FROM source s;
```

As one can note, currently the reading and writing are treated separately but we're working on unifying the two and automatically translating [HiveQL][] to OpenSearch queries.

### Signing requests for IAM authentication

Signing requests would require the [aws-sdk-bundle](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle) in your job classpath.

```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.opensearch.hadoop.hive.OpenSearchStorageHandler'
TBLPROPERTIES(
    'opensearch.nodes' = 'https://search-xxx.us-east-1.es.amazonaws.com',
    'opensearch.port' = '443',
    'opensearch.net.ssl' = 'true',
    'opensearch.resource' = 'artists',
    'opensearch.nodes.wan.only' = 'true',
    'opensearch.aws.sigv4.enabled' = 'true',
    'opensearch.aws.sigv4.region' = 'us-east-1'
    );
```

## [Apache Pig][]
OpenSearch-Hadoop provides both read and write functions for Pig so you can access OpenSearch from Pig scripts.

Register OpenSearch-Hadoop jar into your script or add it to your Pig classpath:
```
REGISTER /path_to_jar/opensearch-hadoop-<version>.jar;
```
Additionally one can define an alias to save some chars:
```
%define OPENSEARCHSTORAGE org.opensearch.pig.hadoop.OpenSearchStorage()
```
and use `$OPENSEARCHSTORAGE` for storage definition.

### Reading
To read data from OpenSearch, use `OpenSearchStorage` and specify the query through the `LOAD` function:
```SQL
A = LOAD 'radio/artists' USING org.opensearch.pig.hadoop.OpenSearchStorage('opensearch.query=?q=me*');
DUMP A;
```

### Writing
Use the same `Storage` to write data to OpenSearch:
```SQL
A = LOAD 'src/artists.dat' USING PigStorage() AS (id:long, name, url:chararray, picture: chararray);
B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;
STORE B INTO 'radio/artists' USING org.opensearch.pig.hadoop.OpenSearchStorage();
```
## [Apache Spark][]
OpenSearch-Hadoop provides native (Java and Scala) integration with Spark: for reading a dedicated `RDD` and for writing, methods that work on any `RDD`. Spark SQL is also supported

### Scala

### Reading
To read data from OpenSearch, create a dedicated `RDD` and specify the query as an argument:

```scala
import org.opensearch.spark._

..
val conf = ...
val sc = new SparkContext(conf)
sc.opensearchRDD("radio/artists", "?q=me*")
```

#### Spark SQL
```scala
import org.opensearch.spark.sql._

// DataFrame schema automatically inferred
val df = sqlContext.read.format("opensearch").load("buckethead/albums")

// operations get pushed down and translated at runtime to OpenSearch QueryDSL
val playlist = df.filter(df("category").equalTo("pikes").and(df("year").geq(2016)))
```

### Writing
Import the `org.opensearch.spark._` package to gain `savetoEs` methods on your `RDD`s:

```scala
import org.opensearch.spark._

val conf = ...
val sc = new SparkContext(conf)

val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

sc.makeRDD(Seq(numbers, airports)).saveToOpenSearch("spark/docs")
```

#### Spark SQL

```scala
import org.opensearch.spark.sql._

val df = sqlContext.read.json("examples/people.json")
df.saveToOpenSearch("spark/people")
```

### Java

In a Java environment, use the `org.opensearch.spark.rdd.java.api` package, in particular the `JavaOpenSearchSpark` class.

### Reading
To read data from OpenSearch, create a dedicated `RDD` and specify the query as an argument.

```java
import org.apache.spark.api.java.JavaSparkContext;
import org.opensearch.spark.rdd.api.java.JavaOpenSearchSpark;

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);

JavaPairRDD<String, Map<String, Object>> opensearchRDD = JavaOpenSearchSpark.opensearchRDD(jsc, "radio/artists");
```

#### Spark SQL

```java
SQLContext sql = new SQLContext(sc);
DataFrame df = sql.read().format("opensearch").load("buckethead/albums");
DataFrame playlist = df.filter(df.col("category").equalTo("pikes").and(df.col("year").geq(2016)))
```

### Writing

Use `JavaOpenSearchSpark` to index any `RDD` to OpenSearch:
```java
import org.opensearch.spark.rdd.api.java.JavaOpenSearchSpark;

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);

Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
JavaOpenSearchSpark.saveToOpenSearch(javaRDD, "spark/docs");
```

#### Spark SQL

```java
import org.opensearch.spark.sql.api.java.JavaOpenSearchSparkSQL;

DataFrame df = sqlContext.read.json("examples/people.json")
JavaOpenSearchSparkSQL.saveToOpenSearch(df, "spark/docs")
```

### Signing requests for IAM authentication

Signing requests would require the [aws-sdk-bundle](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle) in your job classpath.

```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.opensearch.spark.sql._

val sql = new SQLContext(sc)

val accountsRead = sql.read.json("/Users/user/release/maximus/data/accounts.json")

val options = Map("pushdown" -> "true",     
"opensearch.nodes" -> "https://aos-2.3-domain", 
"opensearch.aws.sigv4.enabled" -> "true",
"opensearch.aws.sigv4.region" -> "us-west-2",
"opensearch.nodes.resolve.hostname" -> "false",
"opensearch.nodes.wan.only" -> "true",
"opensearch.net.ssl" -> "true")   
accountsRead.saveToOpenSearch("accounts-00001", options)
```

## [Apache Storm][]
OpenSearch-Hadoop provides native integration with Storm: for reading a dedicated `Spout` and for writing a specialized `Bolt`

### Reading
To read data from OpenSearch, use `OpenSearchSpout`:
```java
import org.opensearch.storm.OpenSearchSpout;

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("opensearch-spout", new OpenSearchSpout("storm/docs", "?q=me*"), 5);
builder.setBolt("bolt", new PrinterBolt()).shuffleGrouping("opensearch-spout");
```

### Writing
To index data to OpenSearch, use `OpenSearchBolt`:

```java
import org.opensearch.storm.OpenSearchBolt;

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("spout", new RandomSentenceSpout(), 10);
builder.setBolt("opensearch-bolt", new OpenSearchBolt("storm/docs"), 5).shuffleGrouping("spout");
```

## Building the source

OpenSearch Hadoop uses [Gradle][] for its build system and it is not required to have it installed on your machine. By default (`gradlew`), it automatically builds the package and runs the unit tests. For integration testing, use the `integrationTests` task.
See `gradlew tasks` for more information.

To create a distributable zip, run `gradlew distZip` from the command line; once completed you will find the jar in `build/libs`.

To build the project, JVM 8 (Oracle one is recommended) or higher is required.

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
[Map/Reduce]: http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
[Apache Pig]: http://pig.apache.org
[Apache Hive]: http://hive.apache.org
[Apache Spark]: http://spark.apache.org
[Apache Storm]: http://storm.apache.org
[HiveQL]: http://cwiki.apache.org/confluence/display/Hive/LanguageManual
[external table]: http://cwiki.apache.org/Hive/external-tables.html
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Gradle]: http://www.gradle.org/
[REST]: http://www.elastic.co/guide/en/elasticsearch/reference/current/api-conventions.html
[DistributedCache]: http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/filecache/DistributedCache.html