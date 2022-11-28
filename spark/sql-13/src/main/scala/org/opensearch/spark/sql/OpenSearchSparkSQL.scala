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

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.Map

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_QUERY
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_READ
import org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE_WRITE
import org.opensearch.spark.cfg.SparkSettingsManager
import org.opensearch.hadoop.cfg.PropertiesSettings
import org.opensearch.hadoop.mr.security.HadoopUserProvider
import org.opensearch.hadoop.rest.InitializationUtils
import org.opensearch.hadoop.util.ObjectUtils

@deprecated(message="Support for Apache Spark 1 is deprecated. Use Spark 2 or 3.")
object OpenSearchSparkSQL {

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  @transient private[this] val LOG = LogFactory.getLog(OpenSearchSparkSQL.getClass)

  def esDF(sc: SQLContext): DataFrame = esDF(sc, Map.empty[String, String])
  def esDF(sc: SQLContext, resource: String): DataFrame = esDF(sc, Map(OPENSEARCH_RESOURCE_READ -> resource))
  def esDF(sc: SQLContext, resource: String, query: String): DataFrame = esDF(sc, Map(OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))
  def esDF(sc: SQLContext, cfg: Map[String, String]): DataFrame = {
    val esConf = new SparkSettingsManager().load(sc.sparkContext.getConf).copy()
    esConf.merge(cfg.asJava)

    sc.read.format("org.elasticsearch.spark.sql").options(esConf.asProperties.asScala.toMap).load
  }

  def esDF(sc: SQLContext, resource: String, query: String, cfg: Map[String, String]): DataFrame = {
    esDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource, OPENSEARCH_QUERY -> query))
  }

  def esDF(sc: SQLContext, resource: String, cfg: Map[String, String]): DataFrame = {
    esDF(sc, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_READ -> resource))
  }


  def saveToEs(srdd: DataFrame, resource: String): Unit = {
    saveToEs(srdd, Map(OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: DataFrame, resource: String, cfg: Map[String, String]): Unit = {
    saveToEs(srdd, collection.mutable.Map(cfg.toSeq: _*) += (OPENSEARCH_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: DataFrame, cfg: Map[String, String]): Unit = {
    if (srdd != null) {
      val sparkCtx = srdd.sqlContext.sparkContext
      val sparkCfg = new SparkSettingsManager().load(sparkCtx.getConf)
      val esCfg = new PropertiesSettings().load(sparkCfg.save())
      esCfg.merge(cfg.asJava)

      // Need to discover es version before checking index existence
      InitializationUtils.setUserProviderIfNotSet(esCfg, classOf[HadoopUserProvider], LOG)
      InitializationUtils.discoverClusterInfo(esCfg, LOG)
      InitializationUtils.checkIdForOperation(esCfg)
      InitializationUtils.checkIndexExistence(esCfg)

      sparkCtx.runJob(srdd.rdd, new OpenSearchDataFrameWriter(srdd.schema, esCfg.save()).write _)
    }
  }
}