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

package org.opensearch.hadoop.qa.kerberos.spark

import java.security.PrivilegedExceptionAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.opensearch.hadoop.qa.kerberos.security.KeytabLogin

class LoadToES(args: Array[String]) {

  val sparkConf: SparkConf = new SparkConf().setAppName("LoadToES")
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def run(): Unit = {
    if (!sparkConf.contains(LoadToES.CONF_FIELD_NAMES)) {
      throw new IllegalArgumentException(LoadToES.CONF_FIELD_NAMES + " is required")
    }
    val resource = sparkConf.get("spark.opensearch.resource")
    val fieldNames = sparkConf.get(LoadToES.CONF_FIELD_NAMES).split(",")
    val schema = StructType(fieldNames.map(StructField(_, StringType)))

    val df = spark.sqlContext.read
      .schema(schema)
      .option("sep", "\t")
      .csv(args(0))

    df.rdd.map(row => row.getValuesMap(row.schema.fieldNames)).saveToOpenSearch(s"${resource}_rdd")
    df.saveToOpenSearch(s"${resource}_df")
    df.write.format("opensearch").save(s"${resource}_ds")
  }
}

object LoadToES {
  val CONF_FIELD_NAMES = "spark.load.field.names"

  def main(args: Array[String]): Unit = {
    KeytabLogin.doAfterLogin(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = new LoadToES(args).run()
    })
  }
}