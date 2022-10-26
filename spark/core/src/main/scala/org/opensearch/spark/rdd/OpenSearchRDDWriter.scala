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

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.util.TaskCompletionListener
import org.opensearch.spark.serialization.ScalaMapFieldExtractor
import org.opensearch.spark.serialization.ScalaMetadataExtractor
import org.opensearch.spark.serialization.ScalaValueWriter
import org.opensearch.hadoop.cfg.{PropertiesSettings, Settings}
import org.opensearch.hadoop.mr.security.HadoopUserProvider
import org.opensearch.hadoop.rest.{InitializationUtils, RestService}
import org.opensearch.hadoop.security.UserProvider
import org.opensearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.opensearch.hadoop.serialization.builder.ValueWriter
import org.opensearch.hadoop.serialization.bulk.{MetadataExtractor, PerEntityPoolingMetadataExtractor}
import org.opensearch.hadoop.serialization.field.FieldExtractor
import org.opensearch.hadoop.util.ObjectUtils

import java.util.Locale
import scala.reflect.ClassTag


private[spark] class OpenSearchRDDWriter[T: ClassTag](val serializedSettings: String,
                                                      val runtimeMetadata: Boolean = false)
  extends Serializable {

  @transient protected lazy val log: Log = LogFactory.getLog(this.getClass)

  lazy val settings: Settings = {
    val settings = new PropertiesSettings().load(serializedSettings)

    InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
    InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
    InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)
    InitializationUtils.setMetadataExtractorIfNotSet(settings, metadataExtractor, log)
    InitializationUtils.setUserProviderIfNotSet(settings, userProvider, log)

    settings
  }

  lazy val metaExtractor = ObjectUtils.instantiate[MetadataExtractor](settings.getMappingMetadataExtractorClassName, settings)

  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    if (settings.getOpaqueId() != null && settings.getOpaqueId().contains("] [task attempt ") == false) {
      settings.setOpaqueId(String.format(Locale.ROOT, "%s [stage %s] [task attempt %s]", settings.getOpaqueId(),
        taskContext.stageId().toString, taskContext.taskAttemptId.toString))
    }
    val writer = RestService.createWriter(settings, taskContext.partitionId.toLong, -1, log)

    val listener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = writer.close()
    }
    taskContext.addTaskCompletionListener(listener)

    if (runtimeMetadata) {
      writer.repository.addRuntimeFieldExtractor(metaExtractor)
    }

    while (data.hasNext) {
      writer.repository.writeToIndex(processData(data))
    }
  }

  protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]
  protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]
  protected def metadataExtractor: Class[_ <: MetadataExtractor] = classOf[ScalaMetadataExtractor]
  protected def userProvider: Class[_ <: UserProvider] = classOf[HadoopUserProvider]

  protected def processData(data: Iterator[T]): Any = {
    val next = data.next
    if (runtimeMetadata) {
      // use the key to extract metadata && return the value to be used as the document
      val (key, value) = next
      metaExtractor.setObject(key);
      value
    } else {
      next
    }
  }
}