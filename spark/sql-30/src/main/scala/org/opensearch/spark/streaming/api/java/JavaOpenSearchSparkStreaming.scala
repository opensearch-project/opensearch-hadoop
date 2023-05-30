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

package org.opensearch.spark.streaming.api.java

import java.util.{Map => JMap}

import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.opensearch.spark.streaming.OpenSearchSparkStreaming

object JavaOpenSearchSparkStreaming {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def saveToOpenSearch(ds: JavaDStream[_], resource: String): Unit = OpenSearchSparkStreaming.saveToOpenSearch(ds.dstream, resource)
  def saveToOpenSearch(ds: JavaDStream[_], resource: String, cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveToOpenSearch(ds.dstream, resource, cfg.asScala)
  def saveToOpenSearch(ds: JavaDStream[_], cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveToOpenSearch(ds.dstream, cfg.asScala)

  def saveToOpenSearchWithMeta[K, V](ds: JavaPairDStream[K, V], resource: String): Unit = OpenSearchSparkStreaming.saveToOpenSearchWithMeta(ds.dstream, resource)
  def saveToOpenSearchWithMeta[K, V](ds: JavaPairDStream[K, V], resource: String, cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveToOpenSearchWithMeta(ds.dstream, resource, cfg.asScala)
  def saveToOpenSearchWithMeta[K, V](ds: JavaPairDStream[K, V], cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveToOpenSearchWithMeta(ds.dstream, cfg.asScala)

  def saveJsonToOpenSearch(ds: JavaDStream[String], resource: String): Unit = OpenSearchSparkStreaming.saveJsonToOpenSearch(ds.dstream, resource)
  def saveJsonToOpenSearch(ds: JavaDStream[String], resource: String, cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveJsonToOpenSearch(ds.dstream, resource, cfg.asScala)
  def saveJsonToOpenSearch(ds: JavaDStream[String], cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveJsonToOpenSearch(ds.dstream, cfg.asScala)

  def saveJsonByteArrayToOpenSearch(ds: JavaDStream[Array[Byte]], resource: String): Unit = OpenSearchSparkStreaming.saveJsonToOpenSearch(ds.dstream, resource)
  def saveJsonByteArrayToOpenSearch(ds: JavaDStream[Array[Byte]], resource: String, cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveJsonToOpenSearch(ds.dstream, resource, cfg.asScala)
  def saveJsonByteArrayToOpenSearch(ds: JavaDStream[Array[Byte]], cfg: JMap[String, String]): Unit = OpenSearchSparkStreaming.saveJsonToOpenSearch(ds.dstream, cfg.asScala)
}