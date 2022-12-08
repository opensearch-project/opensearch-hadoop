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
package org.opensearch.spark

import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.opensearch.spark.sql.OpenSearchSparkSQL

package object sql {

  @deprecated(message="Support for Apache Spark 1 is deprecated. Use Spark 2 or 3.")
  implicit def sqlContextFunctions(sc: SQLContext)= new SQLContextFunctions(sc)

  @deprecated(message="Support for Apache Spark 1 is deprecated. Use Spark 2 or 3.")
  class SQLContextFunctions(sc: SQLContext) extends Serializable {
    def esDF() = OpenSearchSparkSQL.esDF(sc)
    def esDF(resource: String) = OpenSearchSparkSQL.esDF(sc, resource)
    def esDF(resource: String, query: String) = OpenSearchSparkSQL.esDF(sc, resource, query)
    def esDF(cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.esDF(sc, cfg)
    def esDF(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.esDF(sc, resource, cfg)
    def esDF(resource: String, query: String, cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.esDF(sc, resource, query, cfg)
  }

  @deprecated(message="Support for Apache Spark 1 is deprecated. Use Spark 2 or 3.")
  implicit def sparkDataFrameFunctions(df: DataFrame) = new SparkDataFrameFunctions(df)

  @deprecated(message="Support for Apache Spark 1 is deprecated. Use Spark 2 or 3.")
  class SparkDataFrameFunctions(df: DataFrame) extends Serializable {
    def saveToOpenSearch(resource: String): Unit = { OpenSearchSparkSQL.saveToOpenSearch(df, resource) }
    def saveToOpenSearch(resource: String, cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSparkSQL.saveToOpenSearch(df, resource, cfg) }
    def saveToOpenSearch(cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSparkSQL.saveToOpenSearch(df, cfg)    }
  }
}