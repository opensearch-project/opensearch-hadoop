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

package org.opensearch.hadoop.gradle

/**
 * Loads the locally available version information from the build source.
 */
class OshVersionProperties {

    public static final String OPENSEARCH_HADOOP_VERSION
    public static final String OPENSEARCH_VERSION
    public static final String LUCENE_VERSION
    public static final String BUILD_TOOLS_VERSION
    public static final Map<String, String> VERSIONS
    static {
        Properties versionProperties = new Properties()
        InputStream propertyStream = OshVersionProperties.class.getResourceAsStream('/opensearch-hadoop-version.properties')
        if (propertyStream == null) {
            throw new RuntimeException("Could not locate the opensearch-hadoop-version.properties file!")
        }
        versionProperties.load(propertyStream)
        OPENSEARCH_HADOOP_VERSION = versionProperties.getProperty('opensearch_hadoop')
        OPENSEARCH_VERSION = versionProperties.getProperty('opensearch')
        LUCENE_VERSION = versionProperties.getProperty('lucene')
        BUILD_TOOLS_VERSION = versionProperties.getProperty('build-tools')
        VERSIONS = new HashMap<>()
        for (String propertyName: versionProperties.stringPropertyNames()) {
            VERSIONS.put(propertyName, versionProperties.getProperty(propertyName))
        }
    }
}