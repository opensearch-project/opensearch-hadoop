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
package org.opensearch.spark.cfg

import org.apache.hadoop.security.UserGroupInformation
import org.opensearch.spark.serialization.ReflectionUtils._
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.Matchers._
import org.apache.spark.SparkConf
import org.opensearch.hadoop.cfg.PropertiesSettings

import java.util.Locale

class SparkConfigTest {

  @Test
  def testProperties(): Unit = {
    val cfg = new SparkConf().set("type", "onegative")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("onegative", props.getProperty("type"))
  }
  
  @Test
  def testSparkProperties(): Unit = {
    val cfg = new SparkConf().set("spark.type", "onegative")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("onegative", props.getProperty("type"))
  }

  @Test
  def testSparkPropertiesOverride(): Unit = {
    val cfg = new SparkConf().set("spark.type", "fail").set("type", "win")
    val settings = new SparkSettingsManager().load(cfg)
    val props = new PropertiesSettings().load(settings.save())
    assertEquals("win", props.getProperty("type"))
  }

  @Test
  def testOpaqueId(): Unit = {
    var cfg = new SparkConf()
    assertEquals(String.format(Locale.ROOT, "[spark] [%s] [] []", UserGroupInformation.getCurrentUser.getShortUserName),
        new SparkSettingsManager().load(cfg).getOpaqueId)
    val appName = "some app"
    val appdId = "some app id"
    cfg = new SparkConf().set("spark.app.name", appName).set("spark.app.id", appdId)
    assertEquals(String.format(Locale.ROOT, "[spark] [%s] [%s] [%s]", UserGroupInformation.getCurrentUser.getShortUserName, appName,
      appdId), new SparkSettingsManager().load(cfg).getOpaqueId)
  }
}