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

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_QUERY
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_READ
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_WRITE
import org.opensearch.spark.cfg.SparkSettingsManager
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException
import org.opensearch.hadoop.cfg.PropertiesSettings
import org.opensearch.hadoop.mr.security.HadoopUserProvider
import org.opensearch.hadoop.rest.InitializationUtils
import org.opensearch.hadoop.util.ObjectUtils

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.Map

object OpenSearchSparkSQL {

  private val init = { ObjectUtils.loadClass("org.opensearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  @transient private[this] val LOG = LogFactory.getLog(OpenSearchSparkSQL.getClass)

  //
  // Read
  //
  
  def openSearchDF(sc: SQLContext): DataFrame = openSearchDF(sc, Map.empty[String, String])
  def openSearchDF(sc: SQLContext, resource: String): DataFrame = openSearchDF(sc, Map(OPENSEARCH_RESOURCE_READ -> resource))
  def openSearchDF(sc: SQLContext, resource: String, query: String): DataFrame = openSearchDF(sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))
  def openSearchDF(sc: SQLContext, cfg: Map[String, String]): DataFrame = {
    val openSearchConf = new SparkSettingsManager().load(sc.sparkContext.getConf).copy()
    openSearchConf.merge(cfg.asJava)

    sc.read.format("org.opensearch.spark.sql").options(openSearchConf.asProperties.asScala.toMap).load
  }

  def openSearchDF(sc: SQLContext, resource: String, query: String, cfg: Map[String, String]): DataFrame = {
    openSearchDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))
  }

  def openSearchDF(sc: SQLContext, resource: String, cfg: Map[String, String]): DataFrame = {
    openSearchDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource))
  }

  // SparkSession variant
  def openSearchDF(ss: SparkSession): DataFrame = openSearchDF(ss.sqlContext, Map.empty[String, String])
  def openSearchDF(ss: SparkSession, resource: String): DataFrame = openSearchDF(ss.sqlContext, Map(OPENSEARCH_RESOURCE_READ -> resource))
  def openSearchDF(ss: SparkSession, resource: String, query: String): DataFrame = openSearchDF(ss.sqlContext, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))
  def openSearchDF(ss: SparkSession, cfg: Map[String, String]): DataFrame = openSearchDF(ss.sqlContext, cfg) 
  def openSearchDF(ss: SparkSession, resource: String, query: String, cfg: Map[String, String]): DataFrame = openSearchDF(ss.sqlContext, resource, query, cfg)
  def openSearchDF(ss: SparkSession, resource: String, cfg: Map[String, String]): DataFrame = openSearchDF(ss.sqlContext, resource, cfg)
  
  //
  // Write
  //
  
  def saveToOpenSearch(srdd: Dataset[_], resource: String): Unit = {
    saveToOpenSearch(srdd, Map(OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearch(srdd: Dataset[_], resource: String, cfg: Map[String, String]): Unit = {
    saveToOpenSearch(srdd, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToOpenSearch(srdd: Dataset[_], cfg: Map[String, String]): Unit = {
    if (srdd != null) {
      if (srdd.isStreaming) {
        throw new OpenSearchHadoopIllegalArgumentException("Streaming Datasets should not be saved with 'saveToOpenSearch()'. Instead, use " +
          "the 'writeStream().format(\"opensearch\").save()' methods.")
      }
      val sparkCtx = srdd.sqlContext.sparkContext
      val sparkCfg = new SparkSettingsManager().load(sparkCtx.getConf)
      val openSearchCfg = new PropertiesSettings().load(sparkCfg.save())
      openSearchCfg.merge(cfg.asJava)

      // Need to discover OpenSearch Version before checking index existence
      InitializationUtils.setUserProviderIfNotSet(openSearchCfg, classOf[HadoopUserProvider], LOG)
      InitializationUtils.discoverClusterInfo(openSearchCfg, LOG)
      InitializationUtils.checkIdForOperation(openSearchCfg)
      InitializationUtils.checkIndexExistence(openSearchCfg)

      sparkCtx.runJob(srdd.toDF().rdd, new OpenSearchDataFrameWriter(srdd.schema, openSearchCfg.save()).write _)
    }
  }
}