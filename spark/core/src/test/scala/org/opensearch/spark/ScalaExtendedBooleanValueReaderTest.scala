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

import org.opensearch.hadoop.serialization.builder.AbstractExtendedBooleanValueReaderTest.ExpectedOutcome
import org.opensearch.spark.serialization.ScalaValueReader
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import org.hamcrest.Matcher
import org.opensearch.hadoop.serialization.builder.{AbstractExtendedBooleanValueReaderTest, ValueReader}

class ScalaExtendedBooleanValueReaderTest(jsonString: String, expected: ExpectedOutcome) extends AbstractExtendedBooleanValueReaderTest(jsonString, expected) {

  def createValueReader: ValueReader = {
    return new ScalaValueReader
  }

  def isTrue: Matcher[AnyRef] = {
    return new BaseMatcher[AnyRef]() {
      override def matches(item: scala.Any): Boolean = item.equals(Boolean.box(true))
      override def describeTo(description: Description): Unit = description.appendText("true")
    }
  }

  def isFalse: Matcher[AnyRef] = {
    return new BaseMatcher[AnyRef] {
      override def matches(item: scala.Any): Boolean = item.equals(Boolean.box(false))
      override def describeTo(description: Description): Unit = description.appendText("false")
    }
  }

  def isNull: Matcher[AnyRef] = {
    return new BaseMatcher[AnyRef] {
      override def matches(item: scala.Any): Boolean = item == null
      override def describeTo(description: Description): Unit = description.appendText("null")
    }
  }
}