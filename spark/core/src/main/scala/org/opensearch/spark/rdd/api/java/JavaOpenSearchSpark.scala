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
package org.opensearch.spark.rdd.api.java

import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaPairRDD.fromRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_OUTPUT_JSON
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_QUERY
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_READ
import org.opensearch.spark.rdd.OpenSearchSpark
import org.opensearch.spark.rdd.JavaOpenSearchRDD

object JavaOpenSearchSpark {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def opensearchRDD(jsc: JavaSparkContext): JavaPairRDD[String, JMap[String, Object]] = fromRDD(new JavaOpenSearchRDD[JMap[String, Object]](jsc.sc))
  def opensearchRDD(jsc: JavaSparkContext, resource: String): JavaPairRDD[String, JMap[String, Object]] =
    fromRDD(new JavaOpenSearchRDD[JMap[String, Object]](jsc.sc, Map(OPENSEARCH_RESOURCE_READ -> resource)))
  def opensearchRDD(jsc: JavaSparkContext, resource: String, query: String): JavaPairRDD[String, JMap[String, Object]] =
    fromRDD(new JavaOpenSearchRDD[JMap[String, Object]](jsc.sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query)))
  def opensearchRDD(jsc: JavaSparkContext, cfg: JMap[String, String]): JavaPairRDD[String, JMap[String, Object]] =
    fromRDD(new JavaOpenSearchRDD[JMap[String, Object]](jsc.sc, cfg.asScala))

  def openSearchJsonRDD(jsc: JavaSparkContext): JavaPairRDD[String, String] = fromRDD(new JavaOpenSearchRDD[String](jsc.sc, Map(OPENSEARCH_OUTPUT_JSON -> true.toString)))
  def openSearchJsonRDD(jsc: JavaSparkContext, resource: String): JavaPairRDD[String, String] =
    fromRDD(new JavaOpenSearchRDD[String](jsc.sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_OUTPUT_JSON -> true.toString)))
  def openSearchJsonRDD(jsc: JavaSparkContext, resource: String, query: String): JavaPairRDD[String, String] =
    fromRDD(new JavaOpenSearchRDD[String](jsc.sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query, OPENSEARCH_OUTPUT_JSON -> true.toString)))
  def openSearchJsonRDD(jsc: JavaSparkContext, cfg: JMap[String, String]): JavaPairRDD[String, String] =
    fromRDD(new JavaOpenSearchRDD[String](jsc.sc, cfg.asScala += (OPENSEARCH_OUTPUT_JSON -> true.toString)))

  def saveToOpenSearch(jrdd: JavaRDD[_], resource: String) = OpenSearchSpark.saveToOpenSearch(jrdd.rdd, resource)
  def saveToOpenSearch(jrdd: JavaRDD[_], resource: String, cfg: JMap[String, String]) = OpenSearchSpark.saveToOpenSearch(jrdd.rdd, resource, cfg.asScala)
  def saveToOpenSearch(jrdd: JavaRDD[_], cfg: JMap[String, String]) = OpenSearchSpark.saveToOpenSearch(jrdd.rdd, cfg.asScala)

  def saveToOpenSearchWithMeta[K, V](jrdd: JavaPairRDD[K, V], resource: String) = OpenSearchSpark.saveToOpenSearchWithMeta(jrdd.rdd, resource)
  def saveToOpenSearchWithMeta[K, V](jrdd: JavaPairRDD[K, V], resource: String, cfg: JMap[String, String]) = OpenSearchSpark.saveToOpenSearchWithMeta(jrdd.rdd, resource, cfg.asScala)
  def saveToOpenSearchWithMeta[K, V](jrdd: JavaPairRDD[K, V], cfg: JMap[String, String]) = OpenSearchSpark.saveToOpenSearchWithMeta(jrdd.rdd, cfg.asScala)

  def saveJsonToOpenSearch(jrdd: JavaRDD[String], resource: String) = OpenSearchSpark.saveJsonToOpenSearch(jrdd.rdd, resource)
  def saveJsonToOpenSearch(jrdd: JavaRDD[String], resource: String, cfg: JMap[String, String]) = OpenSearchSpark.saveJsonToOpenSearch(jrdd.rdd, resource, cfg.asScala)
  def saveJsonToOpenSearch(jrdd: JavaRDD[String], cfg: JMap[String, String]) = OpenSearchSpark.saveJsonToOpenSearch(jrdd.rdd, cfg.asScala)

  def saveJsonByteArrayToOpenSearch(jrdd: JavaRDD[Array[Byte]], resource: String) = OpenSearchSpark.saveJsonToOpenSearch(jrdd.rdd, resource)
  def saveJsonByteArrayToOpenSearch(jrdd: JavaRDD[Array[Byte]], resource: String, cfg: JMap[String, String]) = OpenSearchSpark.saveJsonToOpenSearch(jrdd.rdd, resource, cfg.asScala)
  def saveJsonByteArrayToOpenSearch(jrdd: JavaRDD[Array[Byte]], cfg: JMap[String, String]) = OpenSearchSpark.saveJsonToOpenSearch(jrdd.rdd, cfg.asScala)
}