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
package org.opensearch.hadoop.serialization.field;

import java.util.List;

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.SettingsAware;
import org.opensearch.hadoop.util.StringUtils;

public class ConstantFieldExtractor implements FieldExtractor, SettingsAware {

    public static final String PROPERTY = "org.opensearch.hadoop.serialization.ConstantFieldExtractor.property";
    private List<String> fieldNames;
    private Object value;
    private boolean autoQuote = true;

    @Override
    public final Object field(Object target) {
        return (value != null ? value : (fieldNames == null || fieldNames.isEmpty() ? NOT_FOUND : extractField(target)));
    }

    protected Object extractField(Object target) {
        return NOT_FOUND;
    }

    @Override
    public void setSettings(Settings settings) {
        autoQuote = settings.getMappingConstantAutoQuote();
        String fldName = property(settings);
        if (fldName.startsWith("<") && fldName.endsWith(">")) {
            value = initValue(fldName.substring(1, fldName.length() - 1));
        }
        if (value == null) {
            fieldNames = StringUtils.tokenize(fldName, ".");
            processField(settings, fieldNames);
        }
    }

    protected void processField(Settings settings, List<String> fieldNames) {
    }

    protected Object initValue(String value) {
        return ExtractorUtils.extractConstant(value, autoQuote);
    }

    protected String property(Settings settings) {
        String value = settings.getProperty(PROPERTY);
        return (value == null ? "" : value.trim());
    }

    protected List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public String toString() {
        return String.format("%s for field [%s]", getClass().getSimpleName(), fieldNames);
    }
}