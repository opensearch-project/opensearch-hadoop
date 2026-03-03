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

package org.opensearch.spark.streaming

import java.util.UUID

import org.apache.spark.streaming.dstream.DStream
import org.opensearch.hadoop.cfg.ConfigurationOptions._
import org.opensearch.hadoop.cfg.InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY
import org.opensearch.spark.rdd.OpenSearchSpark

import scala.collection.Map

object OpenSearchSparkStreaming {

  // Save methods
  def saveToOpenSearch(ds: DStream[_], resource: String): Unit = {
    saveToOpenSearch(ds, Map(OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearch(ds: DStream[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearch(ds, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearch(ds: DStream[_], cfg: Map[String, String]): Unit = {
    doSaveToOpenSearch(ds, cfg, hasMeta = false)
  }

  // Save with metadata
  def saveToOpenSearchWithMeta[K,V](ds: DStream[(K,V)], resource: String): Unit = {
    saveToOpenSearchWithMeta(ds, Map(OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearchWithMeta[K,V](ds: DStream[(K,V)], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearchWithMeta(ds, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearchWithMeta[K,V](ds: DStream[(K,V)], cfg: Map[String, String]): Unit = {
    doSaveToOpenSearch(ds, cfg, hasMeta = true)
  }

  // Save as JSON
  def saveJsonToOpenSearch(ds: DStream[_], resource: String): Unit = {
    saveToOpenSearch(ds, resource, Map(OPENSEARCH_INPUT_JSON -> true.toString))
  }
  def saveJsonToOpenSearch(ds: DStream[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearch(ds, resource, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_INPUT_JSON -> true.toString))
  }
  def saveJsonToOpenSearch(ds: DStream[_], cfg: Map[String, String]): Unit = {
    saveToOpenSearch(ds, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_INPUT_JSON -> true.toString))
  }

  // Implementation
  def doSaveToOpenSearch(ds: DStream[_], cfg: Map[String, String], hasMeta: Boolean): Unit = {
    // Set the transport pooling key and delegate to the standard OpenSearchSpark save.
    // IMPORTANT: Do not inline this into the lambda expression below
    val config = collection.mutable.Map(cfg.toSeq: _*) += (INTERNAL_TRANSPORT_POOLING_KEY -> UUID.randomUUID().toString)
    ds.foreachRDD(rdd => OpenSearchSpark.doSaveToOpenSearch(rdd, config, hasMeta))
  }
}