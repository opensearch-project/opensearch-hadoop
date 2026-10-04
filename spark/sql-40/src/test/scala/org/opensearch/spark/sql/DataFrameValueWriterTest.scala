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

package org.opensearch.spark.sql

import java.io.ByteArrayOutputStream
import java.sql.Date
import java.time.Instant
import java.time.LocalDate
import java.util.TimeZone

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test
import org.opensearch.hadoop.cfg.{ConfigurationOptions, Settings}
import org.opensearch.hadoop.serialization.OpenSearchHadoopSerializationException
import org.opensearch.hadoop.serialization.json.JacksonJsonGenerator
import org.opensearch.hadoop.util.TestSettings

class DataFrameValueWriterTest {

  private def serialize(value: Row, schema: StructType): String = serialize(value, schema, null)

  private def serialize(value: Row, schema: StructType, settings: Settings): String = {
    val out = new ByteArrayOutputStream()
    val generator = new JacksonJsonGenerator(out)

    val writer = new DataFrameValueWriter()
    if (settings != null) {
      writer.setSettings(settings)
    }
    val result = writer.write((value, schema), generator)
    if (result.isSuccesful == false) {
      throw new OpenSearchHadoopSerializationException("Could not serialize [" + result.getUnknownValue + "]")
    }
    generator.flush()

    new String(out.toByteArray)
  }

  case class SimpleCaseClass(s: String)
  class Garbage(i: Int) {
    def doNothing(): Unit = ()
  }

  @Test
  def testSimpleRow(): Unit = {
    val schema = StructType(Seq(StructField("a", StringType)))
    val row = Row("b")
    assertEquals("""{"a":"b"}""", serialize(row, schema))
  }

  @Test
  def testPrimitiveArray(): Unit = {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val row = Row(Array(1,2,3))
    assertEquals("""{"a":[1,2,3]}""", serialize(row, schema))
  }

  @Test
  def testPrimitiveSeq(): Unit = {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val row = Row(Seq(1,2,3))
    assertEquals("""{"a":[1,2,3]}""", serialize(row, schema))
  }

  @Test
  def testMapInArray(): Unit = {
    val schema = StructType(Seq(StructField("s", ArrayType(MapType(StringType, StringType)))))
    val row = Row(Array(Map("a" -> "b")))
    assertEquals("""{"s":[{"a":"b"}]}""", serialize(row, schema))
  }

  @Test
  def testMapInSeq(): Unit = {
    val schema = StructType(Seq(StructField("s", ArrayType(MapType(StringType, StringType)))))
    val row = Row(Seq(Map("a" -> "b")))
    assertEquals("""{"s":[{"a":"b"}]}""", serialize(row, schema))
  }

  @Test
  @Ignore("SparkSQL uses encoders internally to convert a case class into a Row object. We wont ever see this case.")
  def testCaseClass(): Unit = {
    val schema = StructType(Seq(StructField("a", ScalaReflection.schemaFor[SimpleCaseClass].dataType.asInstanceOf[StructType])))
    val row = Row(SimpleCaseClass("foo"))
    assertEquals("""{"a":{"s":"foo"}}""", serialize(row, schema))
  }

  @Test
  def testIgnoreScalaToJavaToScalaFieldExclusion(): Unit = {
    val settings = new TestSettings()
    settings.setProperty(ConfigurationOptions.OPENSEARCH_MAPPING_EXCLUDE, "skey.ignoreme")

    val schema = StructType(Seq(StructField("skey", StructType(Seq(StructField("jkey", StringType), StructField("ignoreme", StringType))))))
    val row = Row(Row("value", "value"))

    val serialized = serialize(row, schema, settings)
    println(serialized)
    assertEquals("""{"skey":{"jkey":"value"}}""", serialized)
  }

  @Test
  def testWriteMaps(): Unit = {
    val settings = new TestSettings()
    val writer = new DataFrameValueWriter()
    if (settings != null) {
      writer.setSettings(settings)
    }
    {
      val out = new ByteArrayOutputStream()
      val generator = new JacksonJsonGenerator(out)
      val inputMap = Map(("key1", Map(("key2", Array(1, 2, 3)))))
      val result = writer.write(inputMap, generator)
      assertTrue(result.isSuccesful)
      generator.flush()
      assertEquals("{\"key1\":{\"key2\":[1,2,3]}}", new String(out.toByteArray))
    }
    {
      val out = new ByteArrayOutputStream()
      val generator = new JacksonJsonGenerator(out)
      val inputMap = Map(("key1", "value1"), ("key2", "value2"))
      val result = writer.write(inputMap, generator)
      assertTrue(result.isSuccesful)
      generator.flush()
      assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", new String(out.toByteArray))
    }
    {
      val out = new ByteArrayOutputStream()
      val generator = new JacksonJsonGenerator(out)
      val inputMap = Map()
      val result = writer.write(inputMap, generator)
      assertTrue(result.isSuccesful)
      generator.flush()
      assertEquals("{}", new String(out.toByteArray))
    }
  }

  @Test
  def testWriteDatesAndTimestamps(): Unit = {
    val schema = StructType(Seq(
      StructField("t_instant", TimestampType),
      StructField("d_localdate", DateType)
    ))
    val instant = Instant.ofEpochMilli(1690000000000L)
    val localDate = LocalDate.of(2023, 7, 22) 
    val row = Row(instant, localDate)

    val serialized = serialize(row, schema)
    assertTrue(serialized.contains(""""t_instant":1690000000000"""))
    assertTrue(serialized.contains(""""d_localdate":1689984000000"""))
  }

  @Test(expected = classOf[OpenSearchHadoopSerializationException])
  def testWriteDatesAndTimestampsInvalidType(): Unit = {
    val schema = StructType(Seq(StructField("t_instant", TimestampType)))
    val row = Row("this-is-not-a-timestamp") 
    serialize(row, schema)
  }

  @Test
  def testWriteDateUsesUtcStartOfDay(): Unit = {
    val defaultTimeZone = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"))
      val schema = StructType(Seq(StructField("d", DateType)))
      val row = Row(Date.valueOf("2023-07-22"))
      val serialized = serialize(row, schema)
      assertTrue(serialized.contains(""""d":1689984000000"""))
    } finally {
      TimeZone.setDefault(defaultTimeZone)
    }
  }
  }

}