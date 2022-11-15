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

package org.opensearch.spark.serialization.handler.write.imple

import java.util
import org.hamcrest.Matchers
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
import org.junit.Test
import org.opensearch.hadoop.serialization.handler.write.SerializationFailure
import org.opensearch.hadoop.serialization.handler.write.impl.SerializationEventConverter
import org.opensearch.hadoop.util.{DateUtils, StringUtils}

class ScalaSerializationEventConverterTest {

  @Test
  def generateEvent(): Unit = {
    val map = Map("field" -> "value")

    val eventConverter = new SerializationEventConverter

    val iaeFailure = new SerializationFailure(new IllegalArgumentException("garbage"), map, new util.ArrayList[String])

    val rawEvent = eventConverter.getRawEvent(iaeFailure)
    assertThat(rawEvent, Matchers.startsWith("Map(field -> value)"))
    val timestamp = eventConverter.getTimestamp(iaeFailure)
    assertTrue(StringUtils.hasText(timestamp))
    assertTrue(DateUtils.parseDate(timestamp).getTime.getTime > 1L)
    val exceptionType = eventConverter.renderExceptionType(iaeFailure)
    assertEquals("illegal_argument_exception", exceptionType)
    val exceptionMessage = eventConverter.renderExceptionMessage(iaeFailure)
    assertEquals("garbage", exceptionMessage)
    val eventMessage = eventConverter.renderEventMessage(iaeFailure)
    assertEquals("Could not construct bulk entry from record", eventMessage)
  }

}