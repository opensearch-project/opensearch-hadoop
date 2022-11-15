/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
 
package org.opensearch.spark.serialization

import org.opensearch.hadoop.serialization.FieldType.DATE_NANOS
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito
import org.opensearch.hadoop.serialization.Parser

import java.sql.Timestamp
import java.util.Date

class ScalaValueReaderTest {
  @Test
  def testCreateDateNanos(): Unit = {
    val reader = new ScalaValueReader()
    val nanoDate = reader.createDateNanos("2015-01-01T12:10:30.123456789Z")
    assertEquals(1420114230123l, nanoDate.getTime)
    assertEquals(123456789, nanoDate.getNanos)
  }

  @Test
  def testReadValue(): Unit = {
    val reader = new ScalaValueReader()
    val parser = Mockito.mock(classOf[Parser])

    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_STRING)
    val stringValue: String = reader.readValue(parser, "2015-01-01T12:10:30.123456789Z", DATE_NANOS).asInstanceOf[String]
    assertEquals("2015-01-01T12:10:30.123456789Z", stringValue)

    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_NUMBER)
    Mockito.when(parser.longValue()).thenReturn(1420114230123l)
    val dateLong = reader.readValue(parser, "1420114230123", DATE_NANOS).asInstanceOf[Long]
    assertEquals(1420114230123l, dateLong)

    reader.richDate = true
    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_STRING)
    val timestamp = reader.readValue(parser, "2015-01-01T12:10:30.123456789Z", DATE_NANOS).asInstanceOf[Timestamp]
    assertEquals(1420114230123l, timestamp.getTime)
    assertEquals(123456789, timestamp.getNanos)

    Mockito.when(parser.currentToken()).thenReturn(Parser.Token.VALUE_NUMBER)
    Mockito.when(parser.longValue()).thenReturn(1420114230123l)
    val date = reader.readValue(parser, "1420114230123", DATE_NANOS).asInstanceOf[Date]
    assertEquals(1420114230123l, date.getTime)
  }
}