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

import scala.collection.Map
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.opensearch.spark.serialization.ScalaValueReader
import org.opensearch.hadoop.cfg.Settings
import org.opensearch.hadoop.mr.security.HadoopUserProvider
import org.opensearch.hadoop.rest.{InitializationUtils, PartitionDefinition}

import scala.annotation.meta.param

private[spark] class ScalaOpenSearchRDD[T](
  @(transient @param) sc: SparkContext,
  params: Map[String, String] = Map.empty)
  extends AbstractOpenSearchRDD[(String, T)](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaOpenSearchRDDIterator[T] = {
    new ScalaOpenSearchRDDIterator(context, split.asInstanceOf[OpenSearchPartition].opensearchPartition)
  }
}

private[spark] class ScalaOpenSearchRDDIterator[T](
  context: TaskContext,
  partition: PartitionDefinition)
  extends AbstractOpenSearchRDDIterator[(String, T)](context, partition) {

  override def getLogger() = LogFactory.getLog(ScalaOpenSearchRDD.getClass())

  override def initReader(settings: Settings, log: Log) = {
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaValueReader], log)
    InitializationUtils.setUserProviderIfNotSet(settings, classOf[HadoopUserProvider], log)
  }

  override def createValue(value: Array[Object]): (String, T) = {
    (value(0).toString() -> value(1).asInstanceOf[T])
  }
}