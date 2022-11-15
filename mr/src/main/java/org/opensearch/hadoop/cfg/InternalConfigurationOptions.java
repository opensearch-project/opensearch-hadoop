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
package org.opensearch.hadoop.cfg;

/**
 * Property names for internal framework use. They will show up inside the Hadoop
 * configuration or Cascading properties (which act as a distributing support) but not in the API.
 */
public interface InternalConfigurationOptions extends ConfigurationOptions {

    String INTERNAL_OPENSEARCH_TARGET_FIELDS = "opensearch.internal.mr.target.fields";
    // discovered node
    String INTERNAL_OPENSEARCH_DISCOVERED_NODES = "opensearch.internal.discovered.nodes";
    // pinned node
    String INTERNAL_OPENSEARCH_PINNED_NODE = "opensearch.internal.pinned.node";

    String INTERNAL_OPENSEARCH_QUERY_FILTERS = "opensearch.internal.query.filters";

    String INTERNAL_OPENSEARCH_VERSION = "opensearch.internal.opensearch.version";
    String INTERNAL_OPENSEARCH_CLUSTER_NAME = "opensearch.internal.opensearch.cluster.name";
    String INTERNAL_OPENSEARCH_CLUSTER_UUID = "opensearch.internal.opensearch.cluster.uuid";
    // used for isolating connection pools of multiple spark streaming jobs in the same app.
    String INTERNAL_TRANSPORT_POOLING_KEY = "opensearch.internal.transport.pooling.key";

    // don't fetch _source field during scroll queries
    String INTERNAL_OPENSEARCH_EXCLUDE_SOURCE = "opensearch.internal.exclude.source";
    String INTERNAL_OPENSEARCH_EXCLUDE_SOURCE_DEFAULT = "false";
}