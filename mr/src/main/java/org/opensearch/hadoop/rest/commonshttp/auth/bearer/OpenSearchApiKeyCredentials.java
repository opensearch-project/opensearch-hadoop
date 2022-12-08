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

package org.opensearch.hadoop.rest.commonshttp.auth.bearer;

import org.opensearch.hadoop.security.OpenSearchToken;
import org.opensearch.hadoop.security.User;
import org.opensearch.hadoop.security.UserProvider;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Credentials;
import org.opensearch.hadoop.util.Assert;

/**
 * Credentials class that produces an OpenSearchToken if it is available on the currently logged in user.
 */
public class OpenSearchApiKeyCredentials implements Credentials {

    private final UserProvider userProvider;
    private final OpenSearchToken providedToken;
    private final String clusterName;

    public OpenSearchApiKeyCredentials(UserProvider userProvider, String clusterName) {
        Assert.notNull(userProvider, "userProvider must not be null");
        Assert.notNull(clusterName, "clusterName must not be null");
        this.userProvider = userProvider;
        this.providedToken = null;
        this.clusterName = clusterName;
    }

    public OpenSearchApiKeyCredentials(OpenSearchToken providedToken) {
        Assert.notNull(providedToken, "providedToken must not be null");
        this.userProvider = null;
        this.providedToken = providedToken;
        this.clusterName = null;
    }

    public OpenSearchToken getToken() {
        OpenSearchToken opensearchToken = null;
        if (providedToken != null) {
            opensearchToken = providedToken;
        } else {
            User user = userProvider.getUser();
            if (user != null) {
                opensearchToken = user.getOpenSearchToken(clusterName); // Token may be null.
            }
        }
        return opensearchToken;
    }
}