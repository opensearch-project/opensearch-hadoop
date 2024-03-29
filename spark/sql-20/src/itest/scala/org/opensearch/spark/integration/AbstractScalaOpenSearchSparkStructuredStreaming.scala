/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
 
/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.spark.integration

import com.fasterxml.jackson.databind.ObjectMapper

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.{lang => jl}
import java.{util => ju}
import javax.xml.bind.DatatypeConverter
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.Decimal
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_MAPPING_EXCLUDE
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_MAPPING_ID
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_SPARK_DATAFRAME_WRITE_NULL_VALUES
import org.opensearch.hadoop.util.TestUtils.resource
import org.opensearch.hadoop.util.TestUtils.docEndpoint
import org.opensearch.spark.sql.streaming.SparkSqlStreamingConfigs
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.is
import org.hamcrest.Matchers.not
import org.junit.{AfterClass, Assert, Assume, BeforeClass, ClassRule, FixMethodOrder, Rule, Test}
import org.junit.Assert.{assertEquals, assertThat, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.opensearch.hadoop.{OpenSearchAssume, OpenSearchHadoopIllegalArgumentException, OpenSearchHadoopIllegalStateException, TestData}
import org.opensearch.hadoop.cfg.ConfigurationOptions
import org.opensearch.hadoop.rest.RestUtils
import org.opensearch.hadoop.serialization.OpenSearchHadoopSerializationException
import org.opensearch.hadoop.util.{OpenSearchMajorVersion, StringUtils, TestSettings, TestUtils}
import org.opensearch.spark.sql.streaming.StreamingQueryTestHarness

import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Codec
import scala.io.Source

object AbstractScalaOpenSearchSparkStructuredStreaming {

  @transient val appName: String = "opensearch-spark-sql-streaming-test"
  @transient var spark: Option[SparkSession] = None
  @transient val commitLogDir: String = commitLogDirectory()
  @transient val sparkConf: SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local")
    .setAppName(appName)
    .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .setJars(SparkUtils.OPENSEARCH_SPARK_TESTING_JAR)

  @transient @ClassRule val testData = new TestData()

  @BeforeClass
  def setup(): Unit =  {
    sparkConf.setAll(TestSettings.TESTING_PROPS)
    spark = Some(
      SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
    )
  }

  def commitLogDirectory(): String = {
    val tempDir = File.createTempFile("opensearch-spark-structured-streaming", "")
    tempDir.delete()
    tempDir.mkdir()
    val logDir = new File(tempDir, "logs")
    logDir.mkdir()
    logDir.getAbsolutePath
  }

  @AfterClass
  def cleanup(): Unit =  {
    spark.foreach((s: SparkSession) => {
      s.close()
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    })
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("default", jl.Boolean.FALSE))
    list
  }
}

object Products extends Serializable {
  // For sending straight strings
  case class Text(data: String)

  // Basic tuple pair
  case class Record(id: Int, name: String)

  // Meant to model the sampleArtistsDatUri
  case class WrappingRichData(id: Int, name: String, url: String, pictures: String, time: Timestamp, nested: RichData)
  case class RichData(id: Int, name: String, url: String, pictures: String, time: Timestamp)

  // Decimal data holder
  case class DecimalData(decimal: Decimal)
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaOpenSearchSparkStructuredStreaming(prefix: String, something: Boolean) {

  private val tempFolderRule = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = tempFolderRule

  val spark: SparkSession = AbstractScalaOpenSearchSparkStructuredStreaming.spark
    .getOrElse(throw new OpenSearchHadoopIllegalStateException("Spark not started..."))
  val version: OpenSearchMajorVersion = TestUtils.getOpenSearchClusterInfo.getMajorVersion

  import org.opensearch.spark.integration.Products._
  import spark.implicits._

  def wrapIndex(name: String): String = {
    prefix + "-spark-struct-stream-" + name
  }

  def checkpoint(target: String): String = {
    s"${AbstractScalaOpenSearchSparkStructuredStreaming.commitLogDir}/$target"
  }

  def checkpointDir(target: String): String = {
    checkpoint(target)+"/sinks/opensearch"
  }

  @Test
  def test0Framework(): Unit = {
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .runTest {
          test.stream
            .map(_.name)
            .flatMap(_.split(" "))
            .writeStream
            .format("console")
            .start()
        }
  }

  @Test
  def test0FrameworkFailure(): Unit = {
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .expectingToThrow(classOf[StringIndexOutOfBoundsException])
        .runTest {
          test.stream
            .map(_.name)
            .flatMap(_.split(" "))
            .map(_.charAt(-4).toString)
            .writeStream
            .format("console")
            .start()
        }
  }

  @Test(expected = classOf[OpenSearchHadoopIllegalArgumentException])
  def test1FailOnIncorrectSaveCall(): Unit = {
    import org.opensearch.spark.sql._
    val target = wrapIndex(resource("failed-on-save-call", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.stream.saveToOpenSearch(target)

    Assert.fail("Should not launch job with saveToOpenSearch() method")
  }

  @Test(expected = classOf[OpenSearchHadoopIllegalArgumentException])
  def test1FailOnCompleteMode(): Unit = {
    val target = wrapIndex(resource("failed-on-complete-mode", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .runTest {
          test.stream
            .select("name").groupBy("name").count()
            .writeStream
            .outputMode(OutputMode.Complete())
            .option("checkpointLocation", checkpoint(target))
            .format("opensearch")
            .start(target)
        }

    Assert.fail("Should not launch job with Complete mode specified")
  }

  @Test(expected = classOf[OpenSearchHadoopIllegalArgumentException])
  def test1FailOnPartitions(): Unit = {
    val target = wrapIndex(resource("failed-on-partitions", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .runTest {
        test.stream
          .writeStream
          .partitionBy("name")
          .option("checkpointLocation", checkpoint(target))
          .format("opensearch")
          .start(target)
      }

    Assert.fail("Should not launch job with column partition")
  }

  @Test
  def test2BasicWriteWithoutCommitLog(): Unit = {
    val target = wrapIndex(resource("test-basic-write-no-commit", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option(SparkSqlStreamingConfigs.OPENSEARCH_SINK_LOG_ENABLE, "false")
          .option("checkpointLocation", checkpoint(target))
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))

    assertThat(new File(s"${checkpointDir(target)}/0").exists(), not(true))
  }

  @Test
  def test2BasicWrite(): Unit = {
    val target = wrapIndex(resource("test-basic-write", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
        .withInput(Record(2, "Hadoop"))
        .withInput(Record(3, "YARN"))
        .runTest {
          test.stream
              .writeStream
              .option("checkpointLocation", checkpoint(target))
              .format("opensearch")
              .start(target)
        }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))

    Source.fromFile(s"${checkpointDir(target)}/0").getLines().foreach(println)
  }

  @Test
  def test2BasicWriteUsingSessionCommitLog(): Unit = {
    try {
      val check = s"${AbstractScalaOpenSearchSparkStructuredStreaming.commitLogDir}/session1"
      spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key, check)

      val target = wrapIndex(resource("test-basic-write", "data", version))
      val test = new StreamingQueryTestHarness[Record](spark)

      test.withInput(Record(1, "Spark"))
        .withInput(Record(2, "Hadoop"))
        .withInput(Record(3, "YARN"))
        .runTest {
          test.stream
            .writeStream
            .queryName("test-basic-write-session-commit")
            .format("opensearch")
            .start(target)
        }

      assertTrue(RestUtils.exists(target))
      val searchResult = RestUtils.get(target + "/_search?")
      assertThat(searchResult, containsString("Spark"))
      assertThat(searchResult, containsString("Hadoop"))
      assertThat(searchResult, containsString("YARN"))

      Source.fromFile(s"${checkpointDir(target)}/0").getLines().foreach(println)

      assertThat(Files.exists(new File(s"$check/test-basic-write-session-commit/sinks/opensearch/0").toPath), is(true))
    } finally {
      spark.conf.unset(SQLConf.CHECKPOINT_LOCATION.key)
    }
  }

  @Test
  def test2BasicWriteUsingSessionCommitLogNoQueryName(): Unit = {
    try {
      val check = s"${AbstractScalaOpenSearchSparkStructuredStreaming.commitLogDir}/session2"
      spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key, check)

      val target = wrapIndex(resource("test-basic-write", "data", version))
      val test = new StreamingQueryTestHarness[Record](spark)

      test.withInput(Record(1, "Spark"))
        .withInput(Record(2, "Hadoop"))
        .withInput(Record(3, "YARN"))
        .runTest {
          test.stream
            .writeStream
            .format("opensearch")
            .start(target)
        }

      assertTrue(RestUtils.exists(target))
      val searchResult = RestUtils.get(target + "/_search?")
      assertThat(searchResult, containsString("Spark"))
      assertThat(searchResult, containsString("Hadoop"))
      assertThat(searchResult, containsString("YARN"))

      Source.fromFile(s"${checkpointDir(target)}/0").getLines().foreach(println)

      assertThat(Files.exists(new File(check).toPath), is(true))
      assertThat(Files.list(new File(check).toPath).count(), is(2L)) // A UUID for general checkpoint, and one for OpenSearch.
    } finally {
      spark.conf.unset(SQLConf.CHECKPOINT_LOCATION.key)
    }
  }

  @Test(expected = classOf[OpenSearchHadoopIllegalArgumentException])
  def test1FailOnIndexCreationDisabled(): Unit = {
    val target = wrapIndex(resource("test-write-index-create-disabled", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_INDEX_AUTO_CREATE, "no")
          .format("opensearch")
          .start(target)
      }

    assertTrue("Index already exists! Index should not exist prior to this test!", !RestUtils.exists(target))
    Assert.fail("Should not be able to write to index if not already created.")
  }

  @Test
  def test2WriteWithMappingId(): Unit = {
    val target = wrapIndex(resource("test-write-with-id", "data", version))
    val docPath = wrapIndex(docEndpoint("test-write-with-id", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_MAPPING_ID, "id")
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertTrue(RestUtils.exists(docPath + "/1"))
    assertTrue(RestUtils.exists(docPath + "/2"))
    assertTrue(RestUtils.exists(docPath + "/3"))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))
  }

  @Test
  def test2WriteWithMappingExclude(): Unit = {
    val target = wrapIndex(resource("test-write-with-exclude", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_MAPPING_EXCLUDE, "id")
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))
    assertThat(searchResult, not(containsString(""""id":1""")))
  }

  @Test
  def test2WriteToIngestPipeline(): Unit = {
    val pipelineName: String = prefix + "-pipeline"
    val pipeline: String = """{"description":"Test Pipeline","processors":[{"set":{"field":"pipeTEST","value":true,"override":true}}]}"""
    RestUtils.put("/_ingest/pipeline/" + pipelineName, StringUtils.toUTF(pipeline))

    val target = wrapIndex(resource("test-write-ingest", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE, pipelineName)
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    val searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))
    assertThat(searchResult, containsString(""""pipeTEST":true"""))
  }

  @Test
  def test2MultiIndexWrite(): Unit = {
    val target = wrapIndex(resource("test-tech-{name}", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    // Spark passes the checkpoint name to Hadoop's Path class, which encodes the curly braces.
    // The HDFS client doesn't seem to encode this path consistently. It creates the un-encoded
    // file, encodes path name, then checks for the file existing, which fails because the name
    // is different.
    val checkpointName = checkpoint(target.replace("{", "").replace("}", ""))
    Assume.assumeTrue("Checkpoint path is encoded improperly",
      checkpointName.equals(new Path(checkpointName).toUri.toString))

    test.withInput(Record(1, "spark"))
      .withInput(Record(2, "hadoop"))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpointName)
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(wrapIndex(resource("test-tech-spark", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("test-tech-hadoop", "data", version))))
    assertThat(wrapIndex(resource("test-tech-spark", "data", version) + "/_search?"), containsString("spark"))
    assertThat(wrapIndex(resource("test-tech-hadoop", "data", version) + "/_search?"), containsString("hadoop"))
  }

  @Test
  def test2NullValueIgnored() {
    val target = wrapIndex(resource("test-null-data-absent", "data", version))
    val docPath = wrapIndex(docEndpoint("test-null-data-absent", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, null))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_MAPPING_ID, "id")
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(docPath + "/1"), containsString("name"))
    assertThat(RestUtils.get(docPath + "/2"), not(containsString("name")))
  }

  @Test
  def test2NullValueWritten() {
    val target = wrapIndex(resource("test-null-data-null", "data", version))
    val docPath = wrapIndex(docEndpoint("test-null-data-null", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, null))
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_MAPPING_ID, "id")
          .option(OPENSEARCH_SPARK_DATAFRAME_WRITE_NULL_VALUES, "true")
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(docPath + "/1"), containsString("name"))
    assertThat(RestUtils.get(docPath + "/2"), containsString("name"))
  }

  @Test
  def test2WriteWithRichMapping() {
    val target = wrapIndex(resource("test-basic-write-rich-mapping-id", "data", version))
    val docPath = wrapIndex(docEndpoint("test-basic-write-rich-mapping-id", "data", version))
    val test = new StreamingQueryTestHarness[Text](spark)

    Source.fromURI(AbstractScalaOpenSearchSparkStructuredStreaming.testData.sampleArtistsDatUri())(Codec.ISO8859).getLines().foreach(s => test.withInput(Text(s)))

    test
      .runTest {
        test.stream
          .map(_.data.split("\t"))
          .map(a => {
            val id = a(0).toInt
            val name = a(1)
            val url = a(2)
            val pictures = a(3)
            val time = new Timestamp(DatatypeConverter.parseDateTime(a(4)).getTimeInMillis)
            WrappingRichData(id, name, url, pictures, time, RichData(id, name, url, pictures, time))
          })
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_MAPPING_ID, "id")
          .format("opensearch")
          .start(target)
      }

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("345"))
    assertThat(RestUtils.exists(docPath+"/1"), is(true))
  }

  @Test
  def test1FailOnDecimalType() {
    val target = wrapIndex(resource("test-decimal-exception", "data", version))
    val test = new StreamingQueryTestHarness[DecimalData](spark)

    test.withInput(DecimalData(Decimal(10)))
      .expectingToThrow(classOf[OpenSearchHadoopSerializationException])
      .runTest {
        test.stream
          .writeStream
          .option("checkpointLocation", checkpoint(target))
          .format("opensearch")
          .start(target)
      }
  }

  @Test
  def testUpdate(): Unit = {
    val target = wrapIndex(resource("test-update", "data", version))
    val docPath = wrapIndex(docEndpoint("test-update", "data", version))
    val test = new StreamingQueryTestHarness[Record](spark)

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop"))
      .withInput(Record(3, "YARN"))
      .startTest {
        test.stream
          .writeStream
          .outputMode("update")
          .option("checkpointLocation", checkpoint(target))
          .option(OPENSEARCH_MAPPING_ID, "id")
          .format("opensearch")
          .start(target)
      }
    test.waitForPartialCompletion()

    assertTrue(RestUtils.exists(target))
    assertTrue(RestUtils.exists(docPath + "/1"))
    assertTrue(RestUtils.exists(docPath + "/2"))
    assertTrue(RestUtils.exists(docPath + "/3"))
    var searchResult = RestUtils.get(target + "/_search?")
    assertThat(searchResult, containsString("Spark"))
    assertThat(searchResult, containsString("Hadoop"))
    assertThat(searchResult, containsString("YARN"))

    test.withInput(Record(1, "Spark"))
      .withInput(Record(2, "Hadoop2"))
      .withInput(Record(3, "YARN"))
    test.waitForCompletion()
    searchResult = RestUtils.get(target + "/_search?version=true")
    val result: java.util.Map[String, Object] = new ObjectMapper().readValue(searchResult, classOf[java.util.Map[String, Object]])
    val hits = result.get("hits").asInstanceOf[java.util.Map[String, Object]].get("hits").asInstanceOf[java.util.List[java.util.Map[String,
      Object]]].asScala
    hits.foreach(hit => {
      hit.get("_id").asInstanceOf[String] match {
        case "1" => {
          assertEquals(1, hit.get("_version"))
          val value = hit.get("_source").asInstanceOf[java.util.Map[String, Object]].get("name").asInstanceOf[String]
          assertEquals("Spark", value)
        }
        case "2" => {
          assertEquals(2, hit.get("_version")) // The only one that should have been updated
          val value = hit.get("_source").asInstanceOf[java.util.Map[String, Object]].get("name").asInstanceOf[String]
          assertEquals("Hadoop2", value)
        }
        case "3" => {
          assertEquals(1, hit.get("_version"))
          val value = hit.get("_source").asInstanceOf[java.util.Map[String, Object]].get("name").asInstanceOf[String]
          assertEquals("YARN", value)
        }
        case _ => throw new AssertionError("Unexpected result")
      }
    })
  }
}