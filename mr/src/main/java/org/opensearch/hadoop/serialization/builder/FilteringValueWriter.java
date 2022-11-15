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
package org.opensearch.hadoop.serialization.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.SettingsAware;
import org.opensearch.hadoop.serialization.field.FieldFilter;
import org.opensearch.hadoop.util.StringUtils;

import static org.opensearch.hadoop.cfg.ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES_DEFAULT;

public abstract class FilteringValueWriter<T> implements ValueWriter<T>, SettingsAware {

    private List<FieldFilter.NumberedInclude> includes;
    private List<String> excludes;
    private Boolean writeNullValues = Boolean.parseBoolean(ES_SPARK_DATAFRAME_WRITE_NULL_VALUES_DEFAULT);

    @Override
    public void setSettings(Settings settings) {
        List<String> includeAsStrings = StringUtils.tokenize(settings.getMappingIncludes());
        includes = (includeAsStrings.isEmpty() ? Collections.<FieldFilter.NumberedInclude> emptyList() : new ArrayList<FieldFilter.NumberedInclude>(includeAsStrings.size()));
        for (String include : includeAsStrings) {
            includes.add(new FieldFilter.NumberedInclude(include));
        }
        excludes = StringUtils.tokenize(settings.getMappingExcludes());
        writeNullValues = settings.getDataFrameWriteNullValues();
    }

    protected boolean shouldKeep(String parentField, String name) {
        name = StringUtils.hasText(parentField) ? parentField + "." + name : name;
        return FieldFilter.filter(name, includes, excludes).matched;
    }

    protected boolean hasWriteNullValues() {
        return writeNullValues;
    }
}