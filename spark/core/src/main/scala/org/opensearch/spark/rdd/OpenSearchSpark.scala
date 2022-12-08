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
package org.opensearch.spark.rdd

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INPUT_JSON
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_OUTPUT_JSON
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_QUERY
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_READ
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_WRITE
import org.opensearch.spark.cfg.SparkSettingsManager
import org.opensearch.hadoop.cfg.PropertiesSettings
import org.opensearch.hadoop.mr.security.HadoopUserProvider
import org.opensearch.hadoop.rest.InitializationUtils

object OpenSearchSpark {

  @transient private[this] val LOG = LogFactory.getLog(OpenSearchSpark.getClass)

  //
  // Load methods
  //

  def opensearchRDD(sc: SparkContext): RDD[(String, Map[String, AnyRef])] = new ScalaOpenSearchRDD[Map[String, AnyRef]](sc)
  def opensearchRDD(sc: SparkContext, cfg: Map[String, String]): RDD[(String, Map[String, AnyRef])] =
    new ScalaOpenSearchRDD[Map[String, AnyRef]](sc, cfg)
  def opensearchRDD(sc: SparkContext, resource: String): RDD[(String, Map[String, AnyRef])] =
    new ScalaOpenSearchRDD[Map[String, AnyRef]](sc, Map(OPENSEARCH_RESOURCE_READ -> resource))
  def opensearchRDD(sc: SparkContext, resource: String, query: String): RDD[(String, Map[String, AnyRef])] =
    new ScalaOpenSearchRDD[Map[String, AnyRef]](sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))
  def opensearchRDD(sc: SparkContext, resource: String, cfg: Map[String, String]): RDD[(String, Map[String, AnyRef])] =
    new ScalaOpenSearchRDD[Map[String, AnyRef]](sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource))
  def opensearchRDD(sc: SparkContext, resource: String, query: String, cfg: Map[String, String]): RDD[(String, Map[String, AnyRef])] =
    new ScalaOpenSearchRDD[Map[String, AnyRef]](sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))


  // load data as JSON
  def esJsonRDD(sc: SparkContext): RDD[(String, String)] = new ScalaOpenSearchRDD[String](sc, Map(OPENSEARCH_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, cfg: Map[String, String]): RDD[(String, String)] =
    new ScalaOpenSearchRDD[String](sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String): RDD[(String, String)] =
    new ScalaOpenSearchRDD[String](sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, query: String): RDD[(String, String)] =
    new ScalaOpenSearchRDD[String](sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query, OPENSEARCH_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, cfg: Map[String, String]): RDD[(String, String)] =
    new ScalaOpenSearchRDD[String](sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_OUTPUT_JSON -> true.toString))
  def esJsonRDD(sc: SparkContext, resource: String, query: String, cfg: Map[String, String]): RDD[(String, String)] =
    new ScalaOpenSearchRDD[String](sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query, OPENSEARCH_OUTPUT_JSON -> true.toString))


  //
  // Save methods
  //
  def saveToOpenSearch(rdd: RDD[_], resource: String): Unit = { saveToOpenSearch(rdd, Map(OPENSEARCH_RESOURCE_WRITE -> resource)) }
  def saveToOpenSearch(rdd: RDD[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearch(rdd, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearch(rdd: RDD[_], cfg: Map[String, String]): Unit =  {
    doSaveToEs(rdd, cfg, false)
  }

  // Save with metadata
  def saveToOpenSearchWithMeta[K,V](rdd: RDD[(K,V)], resource: String): Unit = { saveToOpenSearchWithMeta(rdd, Map(OPENSEARCH_RESOURCE_WRITE -> resource)) }
  def saveToOpenSearchWithMeta[K,V](rdd: RDD[(K,V)], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearchWithMeta(rdd, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearchWithMeta[K,V](rdd: RDD[(K,V)], cfg: Map[String, String]): Unit = {
    doSaveToEs(rdd, cfg, true)
  }

  private[spark] def doSaveToEs(rdd: RDD[_], cfg: Map[String, String], hasMeta: Boolean): Unit = {
    CompatUtils.warnSchemaRDD(rdd, LogFactory.getLog("org.opensearch.spark.rdd.OpenSearchSpark"))

    if (rdd == null || rdd.partitions.length == 0) {
      return
    }
    
    val sparkCfg = new SparkSettingsManager().load(rdd.sparkContext.getConf)
    val config = new PropertiesSettings().load(sparkCfg.save())
    config.merge(cfg.asJava)

    // Need to discover the EsVersion here before checking if the index exists
    InitializationUtils.setUserProviderIfNotSet(config, classOf[HadoopUserProvider], LOG)
    InitializationUtils.discoverClusterInfo(config, LOG)
    InitializationUtils.checkIdForOperation(config)
    InitializationUtils.checkIndexExistence(config)

    rdd.sparkContext.runJob(rdd, new OpenSearchRDDWriter(config.save(), hasMeta).write _)
  }

  // JSON variant
  def saveJsonToEs(rdd: RDD[_], resource: String): Unit = { saveToOpenSearch(rdd, resource, Map(OPENSEARCH_INPUT_JSON -> true.toString)) }
  def saveJsonToEs(rdd: RDD[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearch(rdd, resource, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_INPUT_JSON -> true.toString))
  }
  def saveJsonToEs(rdd: RDD[_], cfg: Map[String, String]): Unit = {
    saveToOpenSearch(rdd, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_INPUT_JSON -> true.toString))
  }
}