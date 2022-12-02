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
package org.opensearch.hadoop.serialization.bulk;

import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.StringUtils;

import java.util.List;

public class DeleteBulkFactory extends AbstractBulkFactory {


    public DeleteBulkFactory(Settings settings, MetadataExtractor metaExtractor, OpenSearchMajorVersion version) {
        super(settings, metaExtractor, version);
    }

    @Override
    protected String getOperation() {
        return ConfigurationOptions.OPENSEARCH_OPERATION_DELETE;
    }

    @Override
    protected void writeObjectEnd(List<Object> list) {
        // skip adding new-line for each entity as delete doesn't need entity output
        list.add(StringUtils.EMPTY);
    }
}