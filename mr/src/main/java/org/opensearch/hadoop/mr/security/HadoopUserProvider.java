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

package org.opensearch.hadoop.mr.security;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.security.User;
import org.opensearch.hadoop.security.UserProvider;

/**
 * Retrieves the currently logged in Hadoop UGI. Note: If none exists, this will attempt a login.
 */
public class HadoopUserProvider extends UserProvider {

    /**
     * @deprecated Use {@link UserProvider#create(Settings)} instead, or if it is important that you
     * explicitly create a HadoopUserProvider, then use {@link HadoopUserProvider#create(Settings)}
     * instead
     */
    public HadoopUserProvider() {
        super();
    }

    public static HadoopUserProvider create(Settings settings) {
        @SuppressWarnings("deprecation")
        HadoopUserProvider provider = new HadoopUserProvider();
        provider.setSettings(settings);
        return provider;
    }

    @Override
    public User getUser() {
        try {
            return new HadoopUser(UserGroupInformation.getCurrentUser(), getSettings());
        } catch (IOException e) {
            throw new OpenSearchHadoopException("Could not retrieve the current user", e);
        }
    }
}