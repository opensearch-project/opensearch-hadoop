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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.opensearch.spark.rdd.OpenSearchRDDWriter
import org.opensearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.opensearch.hadoop.serialization.builder.ValueWriter
import org.opensearch.hadoop.serialization.field.FieldExtractor

private[spark] class OpenSearchDataFrameWriter
  (schema: StructType, override val serializedSettings: String)
  extends OpenSearchRDDWriter[Row](serializedSettings:String) {

  override protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[DataFrameValueWriter]
  override protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  override protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[DataFrameFieldExtractor]

  override protected def processData(data: Iterator[Row]): Any = { (data.next, schema) }
}