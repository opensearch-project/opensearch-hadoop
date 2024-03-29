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
package org.opensearch.spark

import org.junit.Assert._
import org.opensearch.hadoop.serialization.BaseValueReaderTest
import org.opensearch.spark.serialization.ScalaValueReader

class ScalaValueReaderTest extends BaseValueReaderTest {

    override def createValueReader() = new ScalaValueReader()

    override def checkNull(result: Object): Unit = { assertEquals(null, result)}
    override def checkEmptyString(result: Object): Unit = { assertEquals(null, result)}
    override def checkInteger(result: Object): Unit = { assertEquals(Int.MaxValue, result)}
    override def checkLong(result: Object): Unit = { assertEquals(Long.MaxValue, result)}
    override def checkDouble(result: Object): Unit = { assertEquals(Double.MaxValue, result)}
    override def checkFloat(result: Object): Unit = { assertEquals(Float.MaxValue.toString, result.toString())}
    override def checkBoolean(result: Object): Unit = { assertEquals(Boolean.box(true), result)}
  
}