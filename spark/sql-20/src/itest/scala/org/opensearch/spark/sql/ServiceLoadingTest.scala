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

import java.util.ServiceLoader

import org.apache.spark.sql.sources.DataSourceRegister
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

class ServiceLoadingTest {

  @Test
  def serviceLoadingTest(): Unit = {
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], Thread.currentThread().getContextClassLoader)
    if (serviceLoader.asScala.map(_.shortName()).exists(_.equals("opensearch")) == false) {
      Assert.fail("Cannot locate 'opensearch' data source")
    }
  }

}