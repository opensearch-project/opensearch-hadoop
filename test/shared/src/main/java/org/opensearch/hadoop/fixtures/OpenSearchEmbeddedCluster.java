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
package org.opensearch.hadoop.fixtures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.util.StringUtils;
import org.opensearch.hadoop.util.StringUtils.IpAndPort;

public class OpenSearchEmbeddedCluster {

    /**
     * Comma separated list of http addresses provided by the test fixture in the build system.
     */
    private static final String TESTS_REST_CLUSTER = "tests.rest.cluster";

    /**
     * Set system property to true if you are testing against your own opensearch cluster.
     */
    public static final String DISABLE_LOCAL_OPENSEARCH = "test.disable.local.opensearch";

    private List<IpAndPort> ipAndPorts;

    public OpenSearchEmbeddedCluster() {
        try {
            String nodeAddresses = System.getProperty(TESTS_REST_CLUSTER);
            if (!StringUtils.hasText(nodeAddresses)) {
                // No local OpenSearch stood up. Better throw...
                throw new IllegalStateException("Could not find list of OpenSearch nodes to execute integration " +
                        "tests against. Should you be running tests with an external cluster? Try setting [" +
                        DISABLE_LOCAL_OPENSEARCH + "].");
            }

            List<String> addresses = StringUtils.tokenize(nodeAddresses);
            ipAndPorts = new ArrayList<>(addresses.size());
            for (String address : addresses) {
                ipAndPorts.add(StringUtils.parseIpAddress(address));
            }
            Collections.shuffle(ipAndPorts);
        } catch (Exception e) {
            throw new OpenSearchHadoopException("Encountered exception during embedded node startup", e);
        }
    }

    public List<IpAndPort> getIpAndPort() {
        return ipAndPorts;
    }
}