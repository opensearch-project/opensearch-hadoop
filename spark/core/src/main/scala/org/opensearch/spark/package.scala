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
package org.opensearch

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.opensearch.spark.rdd.OpenSearchSpark
import org.opensearch.hadoop.util.ObjectUtils


package object spark {

  private val init = { ObjectUtils.loadClass("org.opensearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  implicit def sparkContextFunctions(sc: SparkContext)= new SparkContextFunctions(sc)

  class SparkContextFunctions(sc: SparkContext) extends Serializable {
    def opensearchRDD() = OpenSearchSpark.opensearchRDD(sc)
    def opensearchRDD(resource: String) = OpenSearchSpark.opensearchRDD(sc, resource)
    def opensearchRDD(resource: String, query: String) = OpenSearchSpark.opensearchRDD(sc, resource, query)
    def opensearchRDD(cfg: scala.collection.Map[String, String]) = OpenSearchSpark.opensearchRDD(sc, cfg)
    def opensearchRDD(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.opensearchRDD(sc, resource, cfg)
    def opensearchRDD(resource: String, query: String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.opensearchRDD(sc, resource, query, cfg)

    def openSearchJsonRDD() = OpenSearchSpark.openSearchJsonRDD(sc)
    def openSearchJsonRDD(resource: String) = OpenSearchSpark.openSearchJsonRDD(sc, resource)
    def openSearchJsonRDD(resource: String, query: String) = OpenSearchSpark.openSearchJsonRDD(sc, resource, query)
    def openSearchJsonRDD(cfg: scala.collection.Map[String, String]) = OpenSearchSpark.openSearchJsonRDD(sc, cfg)
    def openSearchJsonRDD(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.openSearchJsonRDD(sc, resource, cfg)
    def openSearchJsonRDD(resource: String, query:String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.openSearchJsonRDD(sc, resource, query, cfg)
  }

  implicit def sparkRDDFunctions[T : ClassTag](rdd: RDD[T]) = new SparkRDDFunctions[T](rdd)

  class SparkRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def saveToOpenSearch(resource: String): Unit = { OpenSearchSpark.saveToOpenSearch(rdd, resource) }
    def saveToOpenSearch(resource: String, cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveToOpenSearch(rdd, resource, cfg) }
    def saveToOpenSearch(cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveToOpenSearch(rdd, cfg)    }
  }

  implicit def sparkStringJsonRDDFunctions(rdd: RDD[String]) = new SparkJsonRDDFunctions[String](rdd)
  implicit def sparkByteArrayJsonRDDFunctions(rdd: RDD[Array[Byte]]) = new SparkJsonRDDFunctions[Array[Byte]](rdd)

  class SparkJsonRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def saveJsonToOpenSearch(resource: String): Unit = { OpenSearchSpark.saveJsonToOpenSearch(rdd, resource) }
    def saveJsonToOpenSearch(resource: String, cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveJsonToOpenSearch(rdd, resource, cfg) }
    def saveJsonToOpenSearch(cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveJsonToOpenSearch(rdd, cfg) }
  }

  implicit def sparkPairRDDFunctions[K : ClassTag, V : ClassTag](rdd: RDD[(K,V)]) = new SparkPairRDDFunctions[K,V](rdd)

  class SparkPairRDDFunctions[K : ClassTag, V : ClassTag](rdd: RDD[(K,V)]) extends Serializable {
    def saveToOpenSearchWithMeta[K,V](resource: String): Unit = { OpenSearchSpark.saveToOpenSearchWithMeta(rdd, resource) }
    def saveToOpenSearchWithMeta[K,V](resource: String, cfg: Map[String, String]): Unit = { OpenSearchSpark.saveToOpenSearchWithMeta(rdd, resource, cfg) }
    def saveToOpenSearchWithMeta[K,V](cfg: Map[String, String]): Unit = { OpenSearchSpark.saveToOpenSearchWithMeta(rdd, cfg) }
  }
}