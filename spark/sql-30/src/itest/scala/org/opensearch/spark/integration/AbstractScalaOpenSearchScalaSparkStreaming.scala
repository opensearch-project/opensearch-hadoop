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

import java.util.concurrent.TimeUnit
import java.{lang => jl, util => ju}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListenerOutputOperationCompleted, _}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized.Parameters
import org.junit.runners.{MethodSorters, Parameterized}
import org.opensearch.hadoop.{OpenSearchAssume, OpenSearchHadoopIllegalArgumentException}
import org.opensearch.hadoop.cfg.ConfigurationOptions
import org.opensearch.hadoop.cfg.ConfigurationOptions._
import org.opensearch.hadoop.rest.RestUtils
import org.opensearch.hadoop.util.{OpenSearchMajorVersion, StringUtils, TestSettings, TestUtils}
import org.opensearch.hadoop.util.TestUtils.resource
import org.opensearch.hadoop.util.TestUtils.docEndpoint
import org.opensearch.spark.rdd.OpenSearchSpark
import org.opensearch.spark.rdd.Metadata._
import org.opensearch.spark.serialization.{Bean, Garbage, ModuleCaseClass, ReflectionUtils, Trip}
import org.opensearch.spark.streaming._

import org.opensearch.spark.integration.ScalaUtils.propertiesAsScalaMap
import scala.collection.mutable
import scala.reflect.ClassTag

object AbstractScalaOpenSearchScalaSparkStreaming {
  @transient val conf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local")
    .setAppName("opensearchtest")
    .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m")
    .setJars(SparkUtils.OPENSEARCH_SPARK_TESTING_JAR)
  @transient var sc: SparkContext = null
  @transient var ssc: StreamingContext = null

  @BeforeClass
  def setup(): Unit =  {
    conf.setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS))
    sc = new SparkContext(conf)
  }

  @AfterClass
  def cleanup(): Unit =  {
    if (sc != null) {
      sc.stop
      // give jetty time to clean its act up
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    }
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("stream-default", jl.Boolean.FALSE))
    list
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[Parameterized])
class AbstractScalaOpenSearchScalaSparkStreaming(val prefix: String, readMetadata: jl.Boolean) extends Serializable {

  val sc = AbstractScalaOpenSearchScalaSparkStreaming.sc
  val cfg = Map(ConfigurationOptions.OPENSEARCH_READ_METADATA -> readMetadata.toString)
  val version = TestUtils.getOpenSearchClusterInfo.getMajorVersion
  val keyword = "keyword"

  var ssc: StreamingContext = _

  @Before
  def createStreamingContext(): Unit = {
    ssc = new StreamingContext(sc, Seconds(1))
  }

  @After
  def tearDownStreamingContext(): Unit = {
    if (ssc.getState() != StreamingContextState.STOPPED) {
      ssc.stop(stopSparkContext = false, stopGracefully = true)
    }
  }

  @Test
  def testOpenSearchRDDWriteIndexCreationDisabled(): Unit = {
    val expecting = ExpectingToThrow(classOf[OpenSearchHadoopIllegalArgumentException]).from(ssc)

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-nonexisting-scala-basic-write", "data", version))

    val batch = sc.makeRDD(Seq(doc1, doc2))
    runStream(batch)(_.saveToOpenSearch(target, cfg + (OPENSEARCH_INDEX_AUTO_CREATE -> "no")))

    assertTrue(!RestUtils.exists(target))
    expecting.assertExceptionFound()
  }

  @Test
  def testOpenSearchDataFrame1Write(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" ->(".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-streaming-test-scala-basic-write", "data", version))

    val batch = sc.makeRDD(Seq(doc1, doc2))

    runStream(batch)(_.saveToOpenSearch(target, cfg))

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("OTP"))
    assertThat(RestUtils.get(target + "/_search?"), containsString("two"))
  }

  // @Test
  // def testNestedUnknownCharacter(): Unit = {
  //   val expected = ExpectingToThrow(classOf[SparkException]).from(ssc)
  //   val doc = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A", "B", "C"), "unknown" -> new Garbage(0))
  //   val batch = sc.makeRDD(Seq(doc))
  //   runStream(batch)(_.saveToOpenSearch(wrapIndex(resource("spark-streaming-test-nested-map", "data", version)), cfg))
  //   expected.assertExceptionFound()
  // }

  @Test
  def testOpenSearchRDDWriteCaseClass(): Unit = {
    val javaBean = new Bean("bar", 1, true)
    val caseClass1 = Trip("OTP", "SFO")
    val caseClass2 = ModuleCaseClass(1, "OTP", "MUC")

    val vals = ReflectionUtils.caseClassValues(caseClass2)

    val target = wrapIndex(resource("spark-streaming-test-scala-basic-write-objects", "data", version))

    val batch = sc.makeRDD(Seq(javaBean, caseClass1))
    runStreamRecoverably(batch)(_.saveToOpenSearch(target, cfg))

    val batch2 = sc.makeRDD(Seq(javaBean, caseClass2))
    runStream(batch2)(_.saveToOpenSearch(target, Map("opensearch.mapping.id"->"id")))

    assertTrue(RestUtils.exists(target))
    assertEquals(3, OpenSearchSpark.opensearchRDD(sc, target).count())
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test
  def testOpenSearchRDDWriteWithMappingId(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-streaming-test-scala-id-write", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-streaming-test-scala-id-write", "data", version))

    val batch = sc.makeRDD(Seq(doc1, doc2))
    runStream(batch)(_.saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_ID -> "number")))

    assertEquals(2, OpenSearchSpark.opensearchRDD(sc, target).count())
    assertTrue(RestUtils.exists(docPath + "/1"))
    assertTrue(RestUtils.exists(docPath + "/2"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testOpenSearchRDDWriteWithDynamicMapping(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-streaming-test-scala-dyn-id-write", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-streaming-test-scala-dyn-id-write", "data", version))

    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2)))
    runStream(pairRDD)(_.saveToOpenSearchWithMeta(target, cfg))

    println(RestUtils.get(target + "/_search?"))

    assertEquals(2, OpenSearchSpark.opensearchRDD(sc, target).count())
    assertTrue(RestUtils.exists(docPath + "/3"))
    assertTrue(RestUtils.exists(docPath + "/4"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testOpenSearchRDDWriteWithDynamicMapMapping(): Unit = {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-streaming-test-scala-dyn-id-write-map", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-streaming-test-scala-dyn-id-write-map", "data", version))

    val metadata1 = Map(ID -> 5)
    val metadata2 = Map(ID -> 6, VERSION -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    runStream(pairRDD)(_.saveToOpenSearchWithMeta(target, cfg))

    assertTrue(RestUtils.exists(docPath + "/5"))
    assertTrue(RestUtils.exists(docPath + "/6"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testOpenSearchRDDWriteWithMappingExclude(): Unit = {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = wrapIndex(resource("spark-streaming-test-scala-write-exclude", "data", version))

    val batch = sc.makeRDD(Seq(trip1, trip2))
    runStream(batch)(_.saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_EXCLUDE -> "airport")))

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("business"))
    assertThat(RestUtils.get(target +  "/_search?"), containsString("participants"))
    assertThat(RestUtils.get(target +  "/_search?"), not(containsString("airport")))
  }

  @Test
  def testOpenSearchRDDIngest(): Unit = {

    val client: RestUtils.ExtendedRestClient = new RestUtils.ExtendedRestClient
    val pipelineName: String = prefix + "-pipeline"
    val pipeline: String = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}"
    client.put("/_ingest/pipeline/" + pipelineName, StringUtils.toUTF(pipeline))
    client.close()

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-streaming-test-scala-ingest-write", "data", version))

    val ingestCfg = cfg + (ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE -> pipelineName) + (ConfigurationOptions.OPENSEARCH_NODES_INGEST_ONLY -> "true")

    val batch = sc.makeRDD(Seq(doc1, doc2))
    runStream(batch)(_.saveToOpenSearch(target, ingestCfg))

    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("\"pipeTEST\":true"))
  }


  @Test
  def testOpenSearchMultiIndexRDDWrite(): Unit = {
    val trip1 = Map("reason" -> "business", "airport" -> "sfo")
    val trip2 = Map("participants" -> 5, "airport" -> "otp")

    val target = wrapIndex(resource("spark-streaming-test-trip-{airport}", "data", version))
    val batch = sc.makeRDD(Seq(trip1, trip2))
    runStream(batch)(_.saveToOpenSearch(target, cfg))

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-streaming-test-trip-otp", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-streaming-test-trip-sfo", "data", version))))

    assertThat(RestUtils.get(wrapIndex(resource("spark-streaming-test-trip-sfo", "data", version) + "/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex(resource("spark-streaming-test-trip-otp", "data", version) + "/_search?")), containsString("participants"))
  }

  @Test
  def testOpenSearchWriteAsJsonMultiWrite(): Unit = {
    val json1 = "{\"reason\" : \"business\",\"airport\" : \"sfo\"}"
    val json2 = "{\"participants\" : 5,\"airport\" : \"otp\"}"

    val batch = sc.makeRDD(Seq(json1, json2))
    runStreamRecoverably(batch)(_.saveJsonToOpenSearch(wrapIndex(resource("spark-streaming-test-json-{airport}", "data", version)), cfg))

    val json1BA = json1.getBytes()
    val json2BA = json2.getBytes()

    val batch2 = sc.makeRDD(Seq(json1BA, json2BA))
    runStream(batch2)(_.saveJsonToOpenSearch(wrapIndex(resource("spark-streaming-test-json-ba-{airport}", "data", version)), cfg))

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-streaming-test-json-sfo", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-streaming-test-json-otp", "data", version))))

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-streaming-test-json-ba-sfo", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-streaming-test-json-ba-otp", "data", version))))

    assertThat(RestUtils.get(wrapIndex(resource("spark-streaming-test-json-sfo", "data", version) + "/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex(resource("spark-streaming-test-json-otp", "data", version) + "/_search?")), containsString("participants"))
  }

  @Test
  def testOpenSearchRDDWriteWithUpsertScriptUsingBothObjectAndRegularString(): Unit = {
    val mapping = wrapMapping("data", s"""{
                    |    "properties": {
                    |      "id": {
                    |        "type": "$keyword"
                    |      },
                    |      "note": {
                    |        "type": "$keyword"
                    |      },
                    |      "address": {
                    |        "type": "nested",
                    |        "properties": {
                    |          "id":    { "type": "$keyword"  },
                    |          "zipcode": { "type": "$keyword"  }
                    |        }
                    |      }
                    |    }
                    |}""".stripMargin)

    val index = "spark-streaming-test-contact"
    val typed = "data"
    val target = resource(index, typed, version)
    val docPath = docEndpoint(index, typed, version)

    RestUtils.touch(index)
    RestUtils.putMapping(index, typed, mapping.getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$docPath/1", """{ "id" : "1", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))
    RestUtils.postData(s"$docPath/2", """{ "id" : "2", "note": "First", "address": [] }""".getBytes(StringUtils.UTF_8))

    val lang = "painless"
    val props = Map("opensearch.write.operation" -> "upsert",
      "opensearch.input.json" -> "true",
      "opensearch.mapping.id" -> "id",
      "opensearch.update.script.lang" -> lang
    )

    // Upsert a value that should only modify the first document. Modification will add an address entry.
    val lines = sc.makeRDD(List("""{"id":"1","address":{"zipcode":"12345","id":"1"}}"""))
    val up_params = "new_address:address"
    val up_script = "ctx._source.address.add(params.new_address)"
    runStreamRecoverably(lines)(_.saveToOpenSearch(target, props + ("opensearch.update.script.params" -> up_params) + ("opensearch.update.script" -> up_script)))

    // Upsert a value that should only modify the second document. Modification will update the "note" field.
    val notes = sc.makeRDD(List("""{"id":"2","note":"Second"}"""))
    val note_up_params = "new_note:note"
    val note_up_script = "ctx._source.note = params.new_note"
    runStream(notes)(_.saveToOpenSearch(target, props + ("opensearch.update.script.params" -> note_up_params) + ("opensearch.update.script" -> note_up_script)))

    assertTrue(RestUtils.exists(s"$docPath/1"))
    assertThat(RestUtils.get(s"$docPath/1"), both(containsString(""""zipcode":"12345"""")).and(containsString(""""note":"First"""")))

    assertTrue(RestUtils.exists(s"$docPath/2"))
    assertThat(RestUtils.get(s"$docPath/2"), both(not(containsString(""""zipcode":"12345""""))).and(containsString(""""note":"Second"""")))
  }

  @Test
  def testNullAsEmpty(): Unit = {
    val data = Seq(
      Map("field1" -> 5.4, "field2" -> "foo"),
      Map("field2" -> "bar"),
      Map("field1" -> 0.0, "field2" -> "baz")
    )
    val target = wrapIndex(resource("spark-streaming-test-nullasempty", "data", version))
    val batch = sc.makeRDD(data)

    runStream(batch)(_.saveToOpenSearch(target))

    assertEquals(3, OpenSearchSpark.opensearchRDD(sc, target, cfg).count())
  }

  /**
   * Run a streaming job. Streaming jobs for this test case will not be runnable after this method.
   * In cases where you must run multiple streaming jobs in a test case, use runStreamRecoverably
   */
  def runStream[T: ClassTag](rdd: RDD[T])(f: DStream[T] => Unit): Unit = {
    val rddQueue = mutable.Queue(rdd)
    val stream = ssc.queueStream(rddQueue, oneAtATime = true)
    f(stream)
    ssc.start()
    TimeUnit.SECONDS.sleep(2) // Let the stream processing happen
    ssc.stop(stopSparkContext = false, stopGracefully = true)
  }

  /**
   * Run a streaming job in a way that other streaming jobs may be run
   */
  def runStreamRecoverably[T: ClassTag](rdd: RDD[T])(f: DStream[T] => Unit): Unit = {
    val rddQueue = mutable.Queue(rdd)
    val stream = ssc.queueStream(rddQueue, oneAtATime = true)
    f(stream)
    ssc.start()
    TimeUnit.SECONDS.sleep(2) // Let the stream processing happen
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc = new StreamingContext(sc, Seconds(1))
  }

  def wrapIndex(index: String) = {
    prefix + index
  }

  def wrapMapping(typename: String, mapping: String): String = {
    if (TestUtils.isTypelessVersion(version)) {
      mapping
    } else {
      s"""{"$typename":$mapping}"""
    }
  }

  object ExpectingToThrow {
    def apply[T<:Throwable](expected: Class[T]): ExpectingToThrow[T] = new ExpectingToThrow(expected)
  }

  /**
   * Implement a StreamingListener to listen for failed output operations
   * on an StreamingContext. If there's an exception that matches, keep track.
   * At the end of the test you should call assertExceptionFound to note if
   * the expected outcome occurred.
   */
  class ExpectingToThrow[T <: Throwable](expected: Class[T]) extends StreamingListener {
    var foundException: Boolean = false
    var exceptionType: Any = _

    override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
      val exceptionName = outputOperationCompleted.outputOperationInfo.failureReason match {
        case Some(value) => value.substring(0, value.indexOf(':'))
        case None => ""
      }

      foundException = foundException || expected.getCanonicalName.equals(exceptionName)
      exceptionType = if (foundException) exceptionName
    }

    def from(ssc: StreamingContext): ExpectingToThrow[T] = {
      ssc.addStreamingListener(this)
      this
    }

    def assertExceptionFound(): Unit = {
      if (!foundException) {
        exceptionType match {
          case s: String => Assert.fail(s"Expected ${expected.getCanonicalName} but got $s")
          case _ => Assert.fail(s"Expected ${expected.getCanonicalName} but no Exceptions were thrown")
        }
      }
    }
  }
}