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

  private val init = { ObjectUtils.loadClass("org.elasticsearch.spark.rdd.CompatUtils", classOf[ObjectUtils].getClassLoader) }

  implicit def sparkContextFunctions(sc: SparkContext)= new SparkContextFunctions(sc)

  class SparkContextFunctions(sc: SparkContext) extends Serializable {
    def esRDD() = OpenSearchSpark.opensearchRDD(sc)
    def esRDD(resource: String) = OpenSearchSpark.esRDD(sc, resource)
    def esRDD(resource: String, query: String) = OpenSearchSpark.esRDD(sc, resource, query)
    def esRDD(cfg: scala.collection.Map[String, String]) = OpenSearchSpark.esRDD(sc, cfg)
    def esRDD(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.esRDD(sc, resource, cfg)
    def esRDD(resource: String, query: String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.esRDD(sc, resource, query, cfg)

    def esJsonRDD() = OpenSearchSpark.esJsonRDD(sc)
    def esJsonRDD(resource: String) = OpenSearchSpark.esJsonRDD(sc, resource)
    def esJsonRDD(resource: String, query: String) = OpenSearchSpark.esJsonRDD(sc, resource, query)
    def esJsonRDD(cfg: scala.collection.Map[String, String]) = OpenSearchSpark.esJsonRDD(sc, cfg)
    def esJsonRDD(resource: String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.esJsonRDD(sc, resource, cfg)
    def esJsonRDD(resource: String, query:String, cfg: scala.collection.Map[String, String]) = OpenSearchSpark.esJsonRDD(sc, resource, query, cfg)
  }

  implicit def sparkRDDFunctions[T : ClassTag](rdd: RDD[T]) = new SparkRDDFunctions[T](rdd)

  class SparkRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def saveToEs(resource: String): Unit = { OpenSearchSpark.saveToEs(rdd, resource) }
    def saveToEs(resource: String, cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveToEs(rdd, resource, cfg) }
    def saveToEs(cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveToEs(rdd, cfg)    }
  }

  implicit def sparkStringJsonRDDFunctions(rdd: RDD[String]) = new SparkJsonRDDFunctions[String](rdd)
  implicit def sparkByteArrayJsonRDDFunctions(rdd: RDD[Array[Byte]]) = new SparkJsonRDDFunctions[Array[Byte]](rdd)

  class SparkJsonRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def saveJsonToEs(resource: String): Unit = { OpenSearchSpark.saveJsonToEs(rdd, resource) }
    def saveJsonToEs(resource: String, cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveJsonToEs(rdd, resource, cfg) }
    def saveJsonToEs(cfg: scala.collection.Map[String, String]): Unit = { OpenSearchSpark.saveJsonToEs(rdd, cfg) }
  }

  implicit def sparkPairRDDFunctions[K : ClassTag, V : ClassTag](rdd: RDD[(K,V)]) = new SparkPairRDDFunctions[K,V](rdd)

  class SparkPairRDDFunctions[K : ClassTag, V : ClassTag](rdd: RDD[(K,V)]) extends Serializable {
    def saveToEsWithMeta[K,V](resource: String): Unit = { OpenSearchSpark.saveToEsWithMeta(rdd, resource) }
    def saveToEsWithMeta[K,V](resource: String, cfg: Map[String, String]): Unit = { OpenSearchSpark.saveToEsWithMeta(rdd, resource, cfg) }
    def saveToEsWithMeta[K,V](cfg: Map[String, String]): Unit = { OpenSearchSpark.saveToEsWithMeta(rdd, cfg) }
  }
}