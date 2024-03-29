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

import java.util.ArrayList;
import java.util.List;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.SettingsAware;
import org.opensearch.hadoop.serialization.bulk.RawJson;
import org.opensearch.hadoop.util.Assert;
import org.opensearch.hadoop.util.ObjectUtils;
import org.opensearch.hadoop.util.StringUtils;

public abstract class AbstractIndexExtractor implements IndexExtractor, SettingsAware {

    private static final String FORMAT_SEPARATOR = "|";

    protected Settings settings;
    protected String pattern;
    protected boolean hasPattern = false;
    protected List<Object> index;
    protected List<Object> type;

    @Override
    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    @Override
    public void compile(String pattern) {
        this.pattern = pattern;
        // break it down into index/type
        String[] split = pattern.split("/");
        Assert.isTrue(!ObjectUtils.isEmpty(split), "invalid pattern given " + pattern);

        // check pattern
        hasPattern = pattern.contains("{") && pattern.contains("}");
        index = parse(split[0].trim());
        if (split.length > 1) {
            // Assert the pattern is only at most 2, and at the least 1
            Assert.isTrue(split.length == 2, "invalid pattern given " + pattern);
            type = parse(split[1].trim());
        } else {
            type = null;
        }
    }

    protected List<Object> parse(String string) {
        // break it down into fields
        List<Object> template = new ArrayList<Object>();
        while (string.contains("{")) {
            int startPattern = string.indexOf("{");
            template.add(string.substring(0, startPattern));
            int endPattern = string.indexOf("}");
            Assert.isTrue(endPattern > startPattern + 1, "Invalid pattern given " + string);
            String nestedString = string.substring(startPattern + 1, endPattern);
            int separator = nestedString.indexOf(FORMAT_SEPARATOR);
            if (separator > 0) {
                Assert.isTrue(nestedString.length() > separator + 1, "Invalid format given " + nestedString);
                String format = nestedString.substring(separator + 1);
                nestedString = nestedString.substring(0, separator);
                template.add(wrapWithFormatter(format, createFieldExtractor(nestedString)));
            }
            else {
                template.add(createFieldExtractor(nestedString));
            }
            string = string.substring(endPattern + 1).trim();
        }
        if (StringUtils.hasText(string)) {
            template.add(string);
        }
        return template;
    }

    private Object wrapWithFormatter(String format, final FieldExtractor createFieldExtractor) {
        // instantiate field extractor
        final IndexFormatter iformatter = ObjectUtils.instantiate(settings.getMappingIndexFormatterClassName(), settings);
        iformatter.configure(format);
        return new FieldExtractor() {
            @Override
            public Object field(Object target) {
                String string = createFieldExtractor.field(target).toString();
                // typically a string in JSON so remove the quotes
                if (string.startsWith("\"")) {
                    string = string.substring(1);
                }
                if (string.endsWith("\"")) {
                    string = string.substring(0, string.length() - 1);
                }

                // hack: an index will always be a primitive so just call toString (instead of doing JSON parsing)
                // the returned value is not formatted as JSON since : 1. there's no need (it will be picked up down the chain), 2: date formatter depends on it
                return iformatter.format(string);
            }
        };
    }

    private void append(StringBuilder sb, List<Object> list, Object target) {
        for (Object object : list) {
            if (object instanceof FieldExtractor) {
                Object field = ((FieldExtractor) object).field(target);
                if (field == null) {
                    throw new OpenSearchHadoopIllegalArgumentException(String.format("Found null value for pattern element in %s", pattern));
                } else if (field == NOT_FOUND) {
                    throw new OpenSearchHadoopIllegalArgumentException(String.format("Cannot find match for %s", pattern));
                } else {
                    sb.append(StringUtils.jsonEncoding(field.toString()));
                }
            }
            else {
                sb.append(StringUtils.jsonEncoding(object.toString()));
            }
        }
    }

    @Override
    public Object field(Object target) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"_index\":\"");
        append(sb, index, target);
        if (type != null) {
            sb.append("\",");
            sb.append("\"_type\":\"");
            append(sb, type, target);
        }
        sb.append("\"");

        return new RawJson(sb.toString());
    }

    @Override
    public boolean hasPattern() {
        return hasPattern;
    }

    protected abstract FieldExtractor createFieldExtractor(String fieldName);
}