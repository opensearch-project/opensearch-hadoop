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

import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException
import java.util.Collections
import java.util.{Set => JSet}
import org.opensearch.hadoop.cfg.Settings
import org.opensearch.hadoop.serialization.SettingsAware

private[sql] trait RowValueReader extends SettingsAware {

  protected var readMetadata = false
  var metadataField = ""
  // columns for each row (loaded on each new row)
  protected var rowColumnsMap: scala.collection.Map[String, Seq[String]] = Map.empty
  // fields that need to be handled as arrays (in absolute name format)
  protected var arrayFields: JSet[String] = Collections.emptySet()
  protected var sparkRowField = Utils.ROOT_LEVEL_NAME
  protected var currentFieldIsGeo = false
  
  abstract override def setSettings(settings: Settings) = {
    super.setSettings(settings)

    readMetadata = settings.getReadMetadata
    val rowInfo = SchemaUtils.getRowInfo(settings)
    rowColumnsMap = rowInfo._1
    arrayFields = rowInfo._2
  }

  def rowColumns(currentField: String): Seq[String] = {
    rowColumnsMap.get(currentField) match {
      case Some(v) => v
      case None => throw new OpenSearchHadoopIllegalStateException(s"Field '$currentField' not found; typically this occurs with arrays which are not mapped as single value")
    }
  }

  def addToBuffer(esRow: ScalaOpenSearchRow, key: AnyRef, value: Any): Unit = {
    val pos = esRow.rowOrder.indexOf(key.toString())
    if (pos < 0 || pos >= esRow.values.size) {
      // geo types allow fields which are ignored - need to skip these if they are not part of the schema
      if (pos >= 0 || !currentFieldIsGeo) {
        if (key.toString().contains(".")) {
          throw new OpenSearchHadoopIllegalStateException(
            s"Found field '$sparkRowField'. Fields containing dots ('.') are not supported in opensearch-hadoop")
        } else {
          throw new OpenSearchHadoopIllegalStateException(s"Position for '$sparkRowField' not found in row; typically this is caused by a mapping inconsistency")
        }
      }
    } else {
      esRow.values.update(pos, value)
    }
  }
}