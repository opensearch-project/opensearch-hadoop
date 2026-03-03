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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

package object sql {

  implicit def sqlContextFunctions(sc: SQLContext)= new SQLContextFunctions(sc)

  class SQLContextFunctions(sc: SQLContext) extends Serializable {
    def openSearchDF() = OpenSearchSparkSQL.openSearchDF(sc)
    def openSearchDF(resource: String) = OpenSearchSparkSQL.openSearchDF(sc, resource)
    def openSearchDF(resource: String, query: String) = OpenSearchSparkSQL.openSearchDF(sc, resource, query)
    def openSearchDF(cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.openSearchDF(sc, cfg)
    def openSearchDF(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.openSearchDF(sc, resource, cfg)
    def openSearchDF(resource: String, query: String, cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.openSearchDF(sc, resource, query, cfg)
  }

  // the sparkDatasetFunctions already takes care of this
  // but older clients might still import it hence why it's still here
  implicit def sparkDataFrameFunctions(df: DataFrame) = new SparkDataFrameFunctions(df)

  class SparkDataFrameFunctions(df: DataFrame) extends Serializable {
    def saveToOpenSearch(resource: String): Unit = { OpenSearchSparkSQL.saveToOpenSearch(df, resource) }
    def saveToOpenSearch(resource: String, cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSparkSQL.saveToOpenSearch(df, resource, cfg) }
    def saveToOpenSearch(cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSparkSQL.saveToOpenSearch(df, cfg)    }
  }
  
  implicit def sparkSessionFunctions(ss: SparkSession)= new SparkSessionFunctions(ss)
  
  class SparkSessionFunctions(ss: SparkSession) extends Serializable {
    def openSearchDF() = OpenSearchSparkSQL.openSearchDF(ss)
    def openSearchDF(resource: String) = OpenSearchSparkSQL.openSearchDF(ss, resource)
    def openSearchDF(resource: String, query: String) = OpenSearchSparkSQL.openSearchDF(ss, resource, query)
    def openSearchDF(cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.openSearchDF(ss, cfg)
    def openSearchDF(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.openSearchDF(ss, resource, cfg)
    def openSearchDF(resource: String, query: String, cfg: scala.collection.Map[String, String]) = OpenSearchSparkSQL.openSearchDF(ss, resource, query, cfg)
  }

  implicit def sparkDatasetFunctions[T : ClassTag](ds: Dataset[T]) = new SparkDatasetFunctions(ds)
  
  class SparkDatasetFunctions[T : ClassTag](ds: Dataset[T]) extends Serializable {
    def saveToOpenSearch(resource: String): Unit =  { OpenSearchSparkSQL.saveToOpenSearch(ds, resource) }
    def saveToOpenSearch(resource: String, cfg: scala.collection.Map[String, String]): Unit =  { OpenSearchSparkSQL.saveToOpenSearch(ds, resource, cfg) }
    def saveToOpenSearch(cfg: scala.collection.Map[String, String]): Unit =  { OpenSearchSparkSQL.saveToOpenSearch(ds, cfg)    }
  }
}