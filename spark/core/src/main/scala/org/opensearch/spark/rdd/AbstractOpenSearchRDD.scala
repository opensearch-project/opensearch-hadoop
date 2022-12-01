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

import JDKCollectionConvertersCompat.Converters._
import scala.reflect.ClassTag
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.opensearch.spark.cfg.SparkSettingsManager
import org.opensearch.hadoop.mr.security.HadoopUserProvider
import org.opensearch.hadoop.rest.{InitializationUtils, PartitionDefinition, RestRepository, RestService}
import org.opensearch.hadoop.util.ObjectUtils

import scala.annotation.meta.param

private[spark] abstract class AbstractOpenSearchRDD[T: ClassTag](
  @(transient @param) sc: SparkContext,
  val params: scala.collection.Map[String, String] = Map.empty)
  extends RDD[T](sc, Nil) {

  private val init = { ObjectUtils.loadClass("org.opensearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  @transient protected lazy val logger = LogFactory.getLog(this.getClass())

  override def getPartitions: Array[Partition] = {
    opensearchPartitions.asScala.zipWithIndex.map { case(esPartition, idx) =>
      new OpenSearchPartition(id, idx, esPartition)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val esSplit = split.asInstanceOf[OpenSearchPartition]
    esSplit.opensearchPartition.getHostNames
  }

  override def checkpoint(): Unit = {
    // Do nothing. OpenSearch RDD should not be checkpointed.
  }

  def esCount(): Long = {
    val repo = new RestRepository(opensearchCfg)
    try {
      repo.count(true)
    } finally {
      repo.close()
    }
  }

  @transient private[spark] lazy val opensearchCfg = {
    val cfg = new SparkSettingsManager().load(sc.getConf).copy();
    cfg.merge(params.asJava)
    InitializationUtils.setUserProviderIfNotSet(cfg, classOf[HadoopUserProvider], logger)
    cfg
  }

  @transient private[spark] lazy val opensearchPartitions = {
    RestService.findPartitions(opensearchCfg, logger)
  }
}

private[spark] class OpenSearchPartition(rddId: Int, idx: Int, val opensearchPartition: PartitionDefinition)
  extends Partition {

  override def hashCode(): Int = 41 * (41 * (41 + rddId) + idx) + opensearchPartition.hashCode()

  override val index: Int = idx
}