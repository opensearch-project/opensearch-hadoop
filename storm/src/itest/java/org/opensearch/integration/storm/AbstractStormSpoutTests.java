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
package org.opensearch.integration.storm;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.storm.cfg.StormConfigurationOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.opensearch.integration.storm.AbstractStormSuite.COMPONENT_HAS_COMPLETED;

@RunWith(Parameterized.class)
public abstract class AbstractStormSpoutTests {

    protected Map conf;
    protected String index;

    public AbstractStormSpoutTests(Map conf, String index) {
        this.conf = conf;
        this.index = index;
        new TestSettings();
        conf.putAll(TestSettings.TESTING_PROPS);
    }

    @Before
    public void setup() {
        // -1 bolt, -1 test
        COMPONENT_HAS_COMPLETED = new Counter(2);
        CapturingBolt.CAPTURED.clear();
    }

    @After
    public void destroy() {
        COMPONENT_HAS_COMPLETED.decrement();
    }

    @Parameters
    public static Collection<Object[]> configs() throws IOException {
        // no ack
        Map noAck = new LinkedHashMap(Collections.singletonMap(StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE, Boolean.FALSE.toString()));

        // read ack
        Map ack = new LinkedHashMap(Collections.singletonMap(StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE, Boolean.TRUE.toString()));

        // read ack bounded queue
        Map ackWithSize = new LinkedHashMap(ImmutableMap.of(StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE, Boolean.TRUE.toString(), StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE_QUEUE_SIZE, "1"));

        // read ack bounded queue with no retries
        Map ackWithSizeNoRetries = new LinkedHashMap(ImmutableMap.of(StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE, Boolean.TRUE.toString(),
                StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE_QUEUE_SIZE, "1",
                StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE, "1",
                StormConfigurationOptions.OPENSEARCH_STORM_SPOUT_FIELDS, "message"));

        return Arrays.asList(new Object[][] {
            { noAck, "storm-spout" },
            { ack, "storm-spout-reliable" },
            //{ ackWithSize, "storm-spout-reliable-size" },
                //{ ackWithSizeNoRetries, "storm-spout-reliable-size-no-retries" } 
        });
    }

}