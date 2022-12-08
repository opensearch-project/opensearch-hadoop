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

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.{lang => jl}
import java.{util => ju}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INDEX_READ_MISSING_AS_EMPTY
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INPUT_JSON
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_MAPPING_EXCLUDE
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_MAPPING_ID
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_MAPPING_JOIN
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_QUERY
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_READ_METADATA
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE
import org.opensearch.hadoop.util.TestUtils.resource
import org.opensearch.hadoop.util.TestUtils.docEndpoint
import org.opensearch.hadoop.rest.RestUtils.ExtendedRestClient
import org.opensearch.spark.rdd.OpenSearchSpark
import org.opensearch.spark.rdd.Metadata.ID
import org.opensearch.spark.rdd.Metadata.TTL
import org.opensearch.spark.rdd.Metadata.VERSION
import org.opensearch.spark.serialization.ReflectionUtils
import org.opensearch.spark.sparkByteArrayJsonRDDFunctions
import org.opensearch.spark.sparkContextFunctions
import org.opensearch.spark.sparkPairRDDFunctions
import org.opensearch.spark.sparkRDDFunctions
import org.opensearch.spark.sparkStringJsonRDDFunctions
import org.hamcrest.Matchers.both
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.not
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Assume.assumeNoException
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import org.opensearch.spark.integration.ScalaUtils.propertiesAsScalaMap
import org.opensearch.spark.rdd.JDKCollectionConvertersCompat.Converters._
import org.opensearch.hadoop.{OpenSearchAssume, OpenSearchHadoopIllegalArgumentException, TestData}
import org.opensearch.hadoop.cfg.ConfigurationOptions
import org.opensearch.hadoop.rest.RestUtils
import org.opensearch.hadoop.serialization.OpenSearchHadoopSerializationException
import org.opensearch.hadoop.util.{OpenSearchMajorVersion, StringUtils, TestSettings, TestUtils}
import org.opensearch.spark.serialization.Bean

object AbstractScalaOpenSearchScalaSpark {
  @transient val conf = new SparkConf()
              .setAppName("opensearchtest")
              .set("spark.io.compression.codec", "lz4")
              .setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
  @transient var cfg: SparkConf = null
  @transient var sc: SparkContext = null

  @ClassRule @transient val testData = new TestData()

  @BeforeClass
  def setup() {
    conf.setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
    sc = new SparkContext(conf)
  }

  @AfterClass
  def cleanup() {
    if (sc != null) {
      sc.stop
      // give jetty time to clean its act up
      Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    }
  }

  @Parameters
  def testParams(): ju.Collection[Array[jl.Object]] = {
    val list = new ju.ArrayList[Array[jl.Object]]()
    list.add(Array("default-", jl.Boolean.FALSE))
    list.add(Array("with-meta-", jl.Boolean.TRUE))
    list
  }

  case class ModuleCaseClass(id: Integer, departure: String, var arrival: String) {
    var l = math.Pi
  }
}

case class Trip(departure: String, arrival: String) {
  var extra = math.Pi
}

class Garbage(i: Int) {
  def doNothing(): Unit = ()
}

@RunWith(classOf[Parameterized])
class AbstractScalaOpenSearchScalaSpark(prefix: String, readMetadata: jl.Boolean) extends Serializable {

  val sc = AbstractScalaOpenSearchScalaSpark.sc
  val cfg = Map(OPENSEARCH_READ_METADATA -> readMetadata.toString())
  val version: OpenSearchMajorVersion = TestUtils.getOpenSearchClusterInfo.getMajorVersion
  val keyword: String = "keyword"

  private def readAsRDD(uri: URI) = {
    // don't use the sc.read.json/textFile to avoid the whole Hadoop madness
    val path = Paths.get(uri)
    // because Windows
    val lines: Seq[String] = Files.readAllLines(path, StandardCharsets.ISO_8859_1).asScala.toSeq
    sc.parallelize(lines)
  }
  
  @Test
  def testBasicRead() {
    val input = AbstractScalaOpenSearchScalaSpark.testData.sampleArtistsDatUri()
    val data = readAsRDD(input).cache()

    assertTrue(data.count > 300)
  }

  @Test
  def testRDDEmptyRead() {
    val target = wrapIndex(resource("spark-test-empty-rdd", "data", version))
    sc.emptyRDD.saveToOpenSearch(target, cfg)
  }

  @Test(expected=classOf[OpenSearchHadoopIllegalArgumentException])
  def testOpenSearchRDDWriteIndexCreationDisabled() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-nonexisting-scala-basic-write", "data", version))

    sc.makeRDD(Seq(doc1, doc2)).saveToOpenSearch(target, collection.mutable.Map(cfg.toSeq: _*) += (
      OPENSEARCH_INDEX_AUTO_CREATE -> "no"))
    assertTrue(!RestUtils.exists(target))
  }
    
  @Test
  def testOpenSearchRDDWrite() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-scala-basic-write", "data", version))

    sc.makeRDD(Seq(doc1, doc2)).saveToOpenSearch(target, cfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  // @Test(expected = classOf[SparkException])
  // def testNestedUnknownCharacter() {
  //   val doc = Map("itemId" -> "1", "map" -> Map("lat" -> 1.23, "lon" -> -70.12), "list" -> ("A", "B", "C"), "unknown" -> new Garbage(0))
  //   sc.makeRDD(Seq(doc)).saveToOpenSearch(wrapIndex(resource("spark-test-nested-map", "data", version)), cfg)
  // }

  @Test
  def testOpenSearchRDDWriteCaseClass() {
    val javaBean = new Bean("bar", 1, true)
    val caseClass1 = Trip("OTP", "SFO")
    val caseClass2 = AbstractScalaOpenSearchScalaSpark.ModuleCaseClass(1, "OTP", "MUC")

    val vals = ReflectionUtils.caseClassValues(caseClass2)

     val target = wrapIndex(resource("spark-test-scala-basic-write-objects", "data", version))

    sc.makeRDD(Seq(javaBean, caseClass1)).saveToOpenSearch(target, cfg)
    sc.makeRDD(Seq(javaBean, caseClass2)).saveToOpenSearch(target, Map("opensearch.mapping.id"->"id"))

    assertTrue(RestUtils.exists(target))
    assertEquals(3, OpenSearchSpark.opensearchRDD(sc, target).count())
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
  }

  @Test
  def testOpenSearchRDDWriteWithMappingId() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = resource("spark-test-scala-id-write", "data", version)
    val docPath = docEndpoint("spark-test-scala-id-write", "data", version)

    sc.makeRDD(Seq(doc1, doc2)).saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_ID -> "number"))

    assertEquals(2, OpenSearchSpark.opensearchRDD(sc, target).count())
    assertTrue(RestUtils.exists(docPath + "/1"))
    assertTrue(RestUtils.exists(docPath + "/2"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }
  
  @Test
  def testOpenSearchRDDWriteWithDynamicMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-test-scala-dyn-id-write", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-test-scala-dyn-id-write", "data", version))

    val pairRDD = sc.makeRDD(Seq((3, doc1), (4, doc2))).saveToOpenSearchWithMeta(target, cfg)

    assertEquals(2, OpenSearchSpark.opensearchRDD(sc, target).count())
    assertTrue(RestUtils.exists(docPath + "/3"))
    assertTrue(RestUtils.exists(docPath + "/4"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test
  def testOpenSearchRDDWriteWithDynamicMapMapping() {
    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-test-scala-dyn-id-write", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-test-scala-dyn-id-write", "data", version))

    val metadata1 = Map(ID -> 5)
    val metadata2 = Map(ID -> 6, VERSION -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    pairRDD.saveToOpenSearchWithMeta(target, cfg)

    assertTrue(RestUtils.exists(docPath + "/5"))
    assertTrue(RestUtils.exists(docPath + "/6"))

    assertThat(RestUtils.get(target + "/_search?"), containsString("SFO"))
  }

  @Test(expected = classOf[OpenSearchHadoopSerializationException])
  def testOpenSearchRDDWriteWithUnsupportedMapping() {

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."), "number" -> 1)
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran", "number" -> 2)

    val target = wrapIndex(resource("spark-test-scala-dyn-id-write-fail", "data", version))

    val metadata1 = Map(ID -> 5)
    val metadata2 = Map(ID -> 6, TTL -> "23")

    assertEquals(5, metadata1.getOrElse(ID, null))
    assertEquals(6, metadata2.getOrElse(ID, null))

    val pairRDD = sc.makeRDD(Seq((metadata1, doc1), (metadata2, doc2)))

    try {
      pairRDD.saveToOpenSearchWithMeta(target, cfg)
    } catch {
      case s: SparkException => throw s.getCause
      case t: Throwable => throw t
    }

    fail("Should not have ingested TTL on ES 6.x+")
  }

  @Test
  def testOpenSearchRDDWriteWithMappingExclude() {
    val trip1 = Map("reason" -> "business", "airport" -> "SFO")
    val trip2 = Map("participants" -> 5, "airport" -> "OTP")

    val target = wrapIndex(resource("spark-test-scala-write-exclude", "data", version))

    sc.makeRDD(Seq(trip1, trip2)).saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_EXCLUDE -> "airport"))
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString("business"))
    assertThat(RestUtils.get(target +  "/_search?"), containsString("participants"))
    assertThat(RestUtils.get(target +  "/_search?"), not(containsString("airport")))
  }

  @Test
  def testOpenSearchRDDWriteJoinField(): Unit = {

    // test mix of short-form and long-form joiner values
    val company1 = Map("id" -> "1", "company" -> "Elastic", "joiner" -> "company")
    val company2 = Map("id" -> "2", "company" -> "Fringe Cafe", "joiner" -> Map("name" -> "company"))
    val company3 = Map("id" -> "3", "company" -> "WATIcorp", "joiner" -> Map("name" -> "company"))

    val employee1 = Map("id" -> "10", "name" -> "kimchy", "joiner" -> Map("name" -> "employee", "parent" -> "1"))
    val employee2 = Map("id" -> "20", "name" -> "April Ryan", "joiner" -> Map("name" -> "employee", "parent" -> "2"))
    val employee3 = Map("id" -> "21", "name" -> "Charlie", "joiner" -> Map("name" -> "employee", "parent" -> "2"))
    val employee4 = Map("id" -> "30", "name" -> "Alvin Peats", "joiner" -> Map("name" -> "employee", "parent" -> "3"))

    val parents = Seq(company1, company2, company3)
    val children = Seq(employee1, employee2, employee3, employee4)
    val docs = parents ++ children

    {
      val index = wrapIndex("spark-test-scala-write-join-separate")
      val typename = "join"
      val target = resource(index, typename, version)
      val getEndpoint = docEndpoint(index, typename, version)

      if (TestUtils.isTypelessVersion(version)) {
        RestUtils.putMapping(index, typename, "data/join/mapping/typeless.json")
      } else {
        RestUtils.putMapping(index, typename, "data/join/mapping/typed.json")
      }

      sc.makeRDD(parents).saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_ID -> "id", OPENSEARCH_MAPPING_JOIN -> "joiner"))
      sc.makeRDD(children).saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_ID -> "id", OPENSEARCH_MAPPING_JOIN -> "joiner"))

      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString("kimchy"))
      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString(""""_routing":"1""""))

      val data = sc.opensearchRDD(target).collectAsMap()

      {
        assertTrue(data.contains("1"))
        val record10 = data("1")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
      }

      {
        assertTrue(data.contains("10"))
        val record10 = data("10")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
        assertTrue(joiner.contains("parent"))
      }

    }
    {
      val index = wrapIndex("spark-test-scala-write-join-combined")
      val typename = "join"
      val target = resource(index, typename, version)
      val getEndpoint = docEndpoint(index, typename, version)

      if (TestUtils.isTypelessVersion(version)) {
        RestUtils.putMapping(index, typename, "data/join/mapping/typeless.json")
      } else {
        RestUtils.putMapping(index, typename, "data/join/mapping/typed.json")
      }

      sc.makeRDD(docs).saveToOpenSearch(target, Map(OPENSEARCH_MAPPING_ID -> "id", OPENSEARCH_MAPPING_JOIN -> "joiner"))

      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString("kimchy"))
      assertThat(RestUtils.get(getEndpoint + "/10?routing=1"), containsString(""""_routing":"1""""))

      val data = sc.opensearchRDD(target).collectAsMap()

      {
        assertTrue(data.contains("1"))
        val record10 = data("1")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
      }

      {
        assertTrue(data.contains("10"))
        val record10 = data("10")
        assertTrue(record10.contains("joiner"))
        val joiner = record10("joiner").asInstanceOf[collection.mutable.Map[String, Object]]
        assertTrue(joiner.contains("name"))
        assertTrue(joiner.contains("parent"))
      }
    }
  }

  @Test
  def testOpenSearchRDDIngest() {
    val client: RestUtils.ExtendedRestClient = new RestUtils.ExtendedRestClient
    val prefix: String = "spark"
    val pipeline: String = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}"
    client.put("/_ingest/pipeline/" + prefix + "-pipeline", StringUtils.toUTF(pipeline))
    client.close();

    val doc1 = Map("one" -> null, "two" -> Set("2"), "three" -> (".", "..", "..."))
    val doc2 = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    val target = wrapIndex(resource("spark-test-scala-ingest-write", "data", version))

    val ingestCfg = cfg + (ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE -> "spark-pipeline") + (ConfigurationOptions.OPENSEARCH_NODES_INGEST_ONLY -> "true")

    sc.makeRDD(Seq(doc1, doc2)).saveToOpenSearch(target, ingestCfg)
    assertTrue(RestUtils.exists(target))
    assertThat(RestUtils.get(target + "/_search?"), containsString(""))
    assertThat(RestUtils.get(target + "/_search?"), containsString("\"pipeTEST\":true"))
  }


  @Test
  def testOpenSearchMultiIndexRDDWrite() {
    val trip1 = Map("reason" -> "business", "airport" -> "sfo")
    val trip2 = Map("participants" -> 5, "airport" -> "otp")

    val target = wrapIndex(resource("spark-test-trip-{airport}", "data", version))
    sc.makeRDD(Seq(trip1, trip2)).saveToOpenSearch(target, cfg)
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-trip-otp", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-trip-sfo", "data", version))))

    assertThat(RestUtils.get(wrapIndex(resource("spark-test-trip-sfo", "data", version) + "/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex(resource("spark-test-trip-otp", "data", version) + "/_search?")), containsString("participants"))
  }

  @Test
  def testOpenSearchWriteAsJsonMultiWrite() {
    val json1 = "{\"reason\" : \"business\",\"airport\" : \"sfo\"}";
    val json2 = "{\"participants\" : 5,\"airport\" : \"otp\"}"

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs(wrapIndex(resource("spark-test-json-{airport}", "data", version)), cfg)

    val json1BA = json1.getBytes()
    val json2BA = json2.getBytes()

    sc.makeRDD(Seq(json1BA, json2BA)).saveJsonToEs(wrapIndex(resource("spark-test-json-ba-{airport}", "data", version)), cfg)

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-sfo", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-otp", "data", version))))

    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-ba-sfo", "data", version))))
    assertTrue(RestUtils.exists(wrapIndex(resource("spark-test-json-ba-otp", "data", version))))

    assertThat(RestUtils.get(wrapIndex(resource("spark-test-json-sfo", "data", version) + "/_search?")), containsString("business"))
    assertThat(RestUtils.get(wrapIndex(resource("spark-test-json-otp", "data", version) + "/_search?")), containsString("participants"))
  }

  @Test(expected = classOf[OpenSearchHadoopIllegalArgumentException ])
  def testOpenSearchRDDBreakOnFileScript(): Unit = {
    val props = Map("opensearch.write.operation" -> "upsert", "opensearch.update.script.file" -> "break")
    val lines = sc.makeRDD(List(Map("id" -> "1")))
    try {
      lines.saveToOpenSearch("should-break", props)
    } catch {
      case s: SparkException => throw s.getCause
      case t: Throwable => throw t
    }
    fail("Should not have succeeded with file script on ES 6x and up.")
  }

  @Test
  def testOpenSearchRDDWriteStoredScriptUpdate(): Unit = {
    val mapping = wrapMapping("data",
      s"""{
        |    "properties": {
        |      "id": {
        |        "type": "$keyword"
        |      },
        |      "counter": {
        |        "type": "long"
        |      }
        |    }
        |}""".stripMargin)

    val index = wrapIndex("spark-test-stored")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)
    RestUtils.touch(index)
    RestUtils.putMapping(index, typename, mapping.getBytes())
    RestUtils.put(s"$docPath/1", """{"id":"1", "counter":5}""".getBytes(StringUtils.UTF_8))

    val scriptName = "increment"
    val lang = "painless"
    val script = "ctx._source.counter = ctx._source.getOrDefault('counter', 0) + 1"

    if (version.onOrAfter(OpenSearchMajorVersion.V_3_X)) {
      RestUtils.put(s"_scripts/$scriptName", s"""{"script":{"lang":"$lang", "source": "$script"}}""".getBytes(StringUtils.UTF_8))
    } else {
      RestUtils.put(s"_scripts/$scriptName", s"""{"script":{"lang":"$lang", "code": "$script"}}""".getBytes(StringUtils.UTF_8))
    }

    val props = Map("opensearch.write.operation" -> "update", "opensearch.mapping.id" -> "id", "opensearch.update.script.stored" -> scriptName)
    val lines = sc.makeRDD(List(Map("id"->"1")))
    lines.saveToOpenSearch(target, props)

    val docs = RestUtils.get(s"$target/_search")
    Assert.assertThat(docs, containsString(""""counter":6"""))
  }

  @Test
  def testOpenSearchRDDWriteWithUpsertScriptUsingBothObjectAndRegularString() {
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

    val index = wrapIndex("spark-test-contact")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)

    RestUtils.putMapping(index, typename, mapping.getBytes())
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
    val up_script = { "ctx._source.address.add(params.new_address)" }
    lines.saveToOpenSearch(target, props + ("opensearch.update.script.params" -> up_params) + ("opensearch.update.script" -> up_script))

    // Upsert a value that should only modify the second document. Modification will update the "note" field.
    val notes = sc.makeRDD(List("""{"id":"2","note":"Second"}"""))
    val note_up_params = "new_note:note"
    val note_up_script = { "ctx._source.note = params.new_note" }
    notes.saveToOpenSearch(target, props + ("opensearch.update.script.params" -> note_up_params) + ("opensearch.update.script" -> note_up_script))

    assertTrue(RestUtils.exists(s"$docPath/1"))
    assertThat(RestUtils.get(s"$docPath/1"), both(containsString(""""zipcode":"12345"""")).and(containsString(""""note":"First"""")))

    assertTrue(RestUtils.exists(s"$docPath/2"))
    assertThat(RestUtils.get(s"$docPath/2"), both(not(containsString(""""zipcode":"12345""""))).and(containsString(""""note":"Second"""")))
  }

  @Test
  def testOpenSearchRDDRead() {
    val target = wrapIndex(resource("spark-test-scala-basic-read", "data", version))
    val docPath = wrapIndex(docEndpoint("spark-test-scala-basic-read", "data", version))
    RestUtils.touch(wrapIndex("spark-test-scala-basic-read"))
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test-scala-basic-read"))

    val esData = OpenSearchSpark.opensearchRDD(sc, target, cfg)
    val messages = esData.filter(doc => doc._2.find(_.toString.contains("message")).nonEmpty)

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testOpenSearchRDDReadQuery() {
    val index = "spark-test-scala-basic-query-read"
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index)

    val queryTarget = resource("*-scala-basic-query-read", typename, version)
    val esData = OpenSearchSpark.opensearchRDD(sc, queryTarget, "?q=message:Hello World", cfg)
    val newData = OpenSearchSpark.opensearchRDD(sc, collection.mutable.Map(cfg.toSeq: _*) += (
      OPENSEARCH_RESOURCE -> queryTarget,
      OPENSEARCH_INPUT_JSON -> "true",
      OPENSEARCH_QUERY -> "?q=message:Hello World"))

    // on each run, 2 docs are added
    assertTrue(esData.count() % 2 == 0)
    assertTrue(newData.count() % 2 == 0)

    assertNotNull(esData.take(10))
    assertNotNull(newData.take(10))
    assertNotNull(esData)
  }

  @Test
  def testOpenSearchRDDReadAsJson() {
    val index = wrapIndex("spark-test-scala-basic-json-read")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(wrapIndex("spark-test-scala-basic-json-read"))

    val esData = OpenSearchSpark.esJsonRDD(sc, target, cfg)
    val messages = esData.filter(doc => doc._2.contains("message"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testOpenSearchRDDReadWithSourceFilter() {
    val index = wrapIndex("spark-test-scala-source-filter-read")
    val typename = "data"
    val target = resource(index, typename, version)
    val docPath = docEndpoint(index, typename, version)

    RestUtils.touch(index)
    RestUtils.postData(docPath, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index)

    val testCfg = cfg + (ConfigurationOptions.OPENSEARCH_READ_SOURCE_FILTER -> "message_date")

    val esData = OpenSearchSpark.opensearchRDD(sc, target, testCfg)
    val messages = esData.filter(doc => doc._2.contains("message_date"))

    assertTrue(messages.count() == 2)
    assertNotNull(messages.take(10))
    assertNotNull(messages)
  }

  @Test
  def testIndexAlias() {
    val doc = """
        | { "number" : 1, "list" : [ "an array", "some value"], "song" : "Golden Eyes" }
        """.stripMargin
    val typename = wrapIndex("type")
    val indexA = wrapIndex("spark-alias-indexa")
    val indexB = wrapIndex("spark-alias-indexb")
    val alias = wrapIndex("spark-alias-alias")
    val aliasTarget = resource(alias, typename, version)
    val docPathA = docEndpoint(indexA, typename, version)
    val docPathB = docEndpoint(indexB, typename, version)

    RestUtils.postData(docPathA + "/1", doc.getBytes())
    RestUtils.postData(docPathB + "/1", doc.getBytes())

    val aliases = """
        |{"actions" : [
          | {"add":{"index":"""".stripMargin + indexA + """" ,"alias": """" + alias + """"}},
          | {"add":{"index":"""".stripMargin + indexB + """" ,"alias": """" + alias + """"}}
        |]}
        """.stripMargin

    println(aliases)
    RestUtils.postData("_aliases", aliases.getBytes())
    RestUtils.refresh(alias)

    val aliasRDD = OpenSearchSpark.esJsonRDD(sc, aliasTarget, cfg)
    assertEquals(2, aliasRDD.count())
  }

  @Test
  def testNullAsEmpty() {
    val data = Seq(
      Map("field1" -> 5.4, "field2" -> "foo"),
      Map("field2" -> "bar"),
      Map("field1" -> 0.0, "field2" -> "baz")
    )
    val index = wrapIndex("spark-test-nullasempty")
    val typename = "data"
    val target = resource(index, typename, version)

    sc.makeRDD(data).saveToOpenSearch(target)

    assertEquals(3, OpenSearchSpark.opensearchRDD(sc, target, cfg).count())
  }

  @Test
  def testNewIndexWithTemplate() {
    val index = wrapIndex("spark-template-index")
    val typename = "alias"
    val target = resource(index, typename, version)

    val indexPattern = "spark-template-*"
    val patternMatchField = s""""index_patterns":["$indexPattern"],"""

    val template = s"""
        |{
        |$patternMatchField
        |"settings" : {
        |    "number_of_shards" : 1,
        |    "number_of_replicas" : 0
        |},
        |"mappings" : ${wrapMapping("alias", s"""{
        |    "properties" : {
        |      "name" : { "type" : "${keyword}" },
        |      "number" : { "type" : "long" },
        |      "@ImportDate" : { "type" : "date" }
        |     }
        |   }}""".stripMargin)},
        |"aliases" : { "spark-temp-index" : {} }
        |}""".stripMargin
    RestUtils.put("_template/" + wrapIndex("test_template"), template.getBytes)

    val rdd = readAsRDD(AbstractScalaOpenSearchScalaSpark.testData.sampleArtistsJsonUri())
    OpenSearchSpark.saveJsonToEs(rdd, target)
    val opensearchRDD = OpenSearchSpark.opensearchRDD(sc, target, cfg)
    println(opensearchRDD.count)
    println(RestUtils.getMappings(index).getResolvedView)

    val erc = new ExtendedRestClient()
    erc.delete("_template/" + wrapIndex("test_template"))
    erc.close()
  }


  @Test
  def testOpenSearchSparkVsScCount() {
    val index = wrapIndex("spark-test-check-counting")
    val typename = "data"
    val target = resource(index, typename, version)

    val rawCore = List( Map("colint" -> 1, "colstr" -> "s"),
                         Map("colint" -> null, "colstr" -> null) )
    sc.parallelize(rawCore, 1).saveToOpenSearch(target)
    val qjson =
      """{"query":{"range":{"colint":{"from":null,"to":"9","include_lower":true,"include_upper":true}}}}"""

    val opensearchRDD = OpenSearchSpark.opensearchRDD(sc, target, qjson)
    val scRDD = sc.opensearchRDD(target, qjson)
    assertEquals(opensearchRDD.collect().size, scRDD.collect().size)
  }


  @Test
  def testaaaaaMultiIndexNonExisting() {

    val multipleMissingIndicesWithSetting = OpenSearchSpark.esJsonRDD(sc, "bumpA,Stump", Map(OPENSEARCH_INDEX_READ_MISSING_AS_EMPTY -> "yes"))
    assertEquals(0, multipleMissingIndicesWithSetting.count)
    val multipleMissingIndicesWithoutSetting = OpenSearchSpark.esJsonRDD(sc, "bumpA,Stump")
    try {
      val count = multipleMissingIndicesWithoutSetting.count
      fail("Should have thrown an exception instead of returning " + count)
    } catch {
      case e: OpenSearchHadoopIllegalArgumentException => //Expected
    }

    val index1 = prefix + "spark-test-read-missing-as-empty-1"
    val typename = "data"
    val target1 = resource(index1, typename, version)
    val docPath1 = docEndpoint(index1, typename, version)

    RestUtils.touch(target1)
    RestUtils.postData(docPath1, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath1, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index1)

    val index2 = prefix + "spark-test-read-missing-as-empty-2"
    val target2 = resource(index2, typename, version)
    val docPath2 = docEndpoint(index2, typename, version)

    RestUtils.touch(target2)
    RestUtils.postData(docPath2, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.postData(docPath2, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes())
    RestUtils.refresh(index2)

    val mixedWithSetting = OpenSearchSpark.esJsonRDD(sc, "bumpA,Stump," + index1 + "," + index2, Map(OPENSEARCH_INDEX_READ_MISSING_AS_EMPTY -> "yes"))
    assertEquals(4, mixedWithSetting.count)
    val mixedWithoutSetting = OpenSearchSpark.esJsonRDD(sc, "bumpA,Stump," + index1)
    try {
      val count = mixedWithoutSetting.count
      fail("Should have thrown an exception instead of returning " + count)
    } catch {
      case e: OpenSearchHadoopIllegalArgumentException => //Expected
    }
  }

  def wrapIndex(index: String) = {
    prefix + index
  }

  def wrapMapping(mappingName: String, mapping: String): String = {
    if (TestUtils.isTypelessVersion(version)) {
      mapping
    } else {
      s"""{"$mappingName":$mapping}"""
    }
  }
}