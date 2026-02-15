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

package org.opensearch.spark.sql.streaming

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.opensearch.spark.rdd.OpenSearchRDDWriter
import org.opensearch.spark.sql.DataFrameFieldExtractor
import org.opensearch.spark.sql.DataFrameValueWriter
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException
import org.opensearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.opensearch.hadoop.serialization.builder.ValueWriter
import org.opensearch.hadoop.serialization.field.FieldExtractor
import org.opensearch.spark.sql.{DataFrameFieldExtractor, DataFrameValueWriter}

/**
 * Takes in iterator of InternalRow objects from a partition of data, writes it to OpenSearch, and manages
 * the streaming commit protocol.
 */
private [sql] class OpenSearchStreamQueryWriter(serializedSettings: String,
                                                schema: StructType,
                                                commitProtocol: OpenSearchCommitProtocol)
  extends OpenSearchRDDWriter[InternalRow](serializedSettings) {

  override protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[DataFrameValueWriter]
  override protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  override protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[DataFrameFieldExtractor]

  private val encoder: ExpressionEncoder[Row] = ExpressionEncoder(RowEncoder.encoderFor(schema))
  private val deserializer: ExpressionEncoder.Deserializer[Row] = encoder.resolveAndBind().createDeserializer()

  override def write(taskContext: TaskContext, data: Iterator[InternalRow]): Unit = {
    // Keep clients from using this method, doesn't return task commit information.
    throw new OpenSearchHadoopIllegalArgumentException("Use run(taskContext, data) instead to retrieve the commit information")
  }

  def run(taskContext: TaskContext, data: Iterator[InternalRow]): TaskCommit = {
    val taskInfo = TaskState(taskContext.partitionId(), settings.getResourceWrite)
    commitProtocol.initTask(taskInfo)
    try {
      super.write(taskContext, data)
    } catch {
      case t: Throwable =>
        commitProtocol.abortTask(taskInfo)
        throw t
    }
    commitProtocol.commitTask(taskInfo)
  }

  override protected def processData(data: Iterator[InternalRow]): Any = {
    val row = deserializer.apply(data.next())
    commitProtocol.recordSeen()
    (row, schema)
  }
}