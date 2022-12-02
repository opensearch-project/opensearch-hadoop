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
package org.opensearch.storm.cfg;


public interface StormConfigurationOptions {

    String OPENSEARCH_STORM_BOLT_TICK_TUPLE_FLUSH = "opensearch.storm.bolt.tick.tuple.flush";
    String OPENSEARCH_STORM_BOLT_TICK_TUPLE_FLUSH_DEFAULT = "true";

    String OPENSEARCH_STORM_BOLT_ACK = "opensearch.storm.bolt.write.ack";
    String OPENSEARCH_STORM_BOLT_ACK_DEFAULT = "false";

    String OPENSEARCH_STORM_BOLT_FLUSH_ENTRIES_SIZE = "opensearch.storm.bolt.flush.entries.size";

    String OPENSEARCH_STORM_SPOUT_RELIABLE = "opensearch.storm.spout.reliable";
    String OPENSEARCH_STORM_SPOUT_RELIABLE_DEFAULT = "false";

    String OPENSEARCH_STORM_SPOUT_RELIABLE_QUEUE_SIZE = "opensearch.storm.spout.reliable.queue.size";
    String OPENSEARCH_STORM_SPOUT_RELIABLE_QUEUE_SIZE_DEFAULT = "0";

    String OPENSEARCH_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE = "opensearch.storm.spout.reliable.retries.per.tuple";
    String OPENSEARCH_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE_DEFAULT = "5";

    String OPENSEARCH_STORM_SPOUT_RELIABLE_TUPLE_FAILURE_HANDLE = "opensearch.storm.spout.reliable.handle.tuple.failure";
    String OPENSEARCH_STORM_SPOUT_RELIABLE_TUPLE_FAILURE_HANDLE_DEFAULT = "abort";

    String OPENSEARCH_STORM_SPOUT_FIELDS = "opensearch.storm.spout.fields";
    String OPENSEARCH_STORM_SPOUT_FIELDS_DEFAULT = "";
}