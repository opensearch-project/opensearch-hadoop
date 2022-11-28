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

import java.util.List;

import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.Assert;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.StringUtils;

class UpdateBulkFactory extends AbstractBulkFactory {

    private final int RETRY_ON_FAILURE;
    private final String RETRY_HEADER;

    private final String SCRIPT;
    private final String SCRIPT_LANG;

    private final boolean HAS_SCRIPT, HAS_LANG, HAS_SCRIPT_UPSERT;
    private final boolean UPSERT;

    public UpdateBulkFactory(Settings settings, MetadataExtractor metaExtractor, OpenSearchMajorVersion opensearchMajorVersion) {
        this(settings, false, metaExtractor, opensearchMajorVersion);
    }

    public UpdateBulkFactory(Settings settings, boolean upsert, MetadataExtractor metaExtractor, OpenSearchMajorVersion opensearchMajorVersion) {
        super(settings, metaExtractor, opensearchMajorVersion);

        UPSERT = upsert;
        RETRY_ON_FAILURE = settings.getUpdateRetryOnConflict();
        RETRY_HEADER = getRequestParameterNames().retryOnConflict + RETRY_ON_FAILURE + "";

        HAS_SCRIPT = settings.hasUpdateScript();
        HAS_SCRIPT_UPSERT = settings.hasScriptUpsert();
        HAS_LANG = StringUtils.hasText(settings.getUpdateScriptLang());

        SCRIPT_LANG = ",\"lang\":\"" + settings.getUpdateScriptLang() + "\"";

        if (HAS_SCRIPT) {
            if (StringUtils.hasText(settings.getUpdateScriptInline())) {
                // INLINE
                String source = "source";
                SCRIPT = "{\"script\":{\"" + source + "\":\"" + settings.getUpdateScriptInline() + "\"";
            } else if (StringUtils.hasText(settings.getUpdateScriptFile())) {
                // FILE
                SCRIPT = "{\"script\":{\"file\":\"" + settings.getUpdateScriptFile() + "\"";
            } else if (StringUtils.hasText(settings.getUpdateScriptStored())) {
                // STORED
                SCRIPT = "{\"script\":{\"stored\":\"" + settings.getUpdateScriptStored() + "\"";
            } else {
                throw new OpenSearchHadoopIllegalStateException("No update script found...");
            }
        } else {
            SCRIPT = null;
        }
    }

    @Override
    protected String getOperation() {
        return ConfigurationOptions.ES_OPERATION_UPDATE;
    }

    @Override
    protected void otherHeader(List<Object> list, boolean commaMightBeNeeded) {
        if (RETRY_ON_FAILURE > 0) {
            if (commaMightBeNeeded) {
                list.add(",");
            }
            list.add(RETRY_HEADER);
        }
    }

    @Override
    protected void writeObjectHeader(List<Object> list) {
        super.writeObjectHeader(list);

        Object paramExtractor = getMetadataExtractorOrFallback(MetadataExtractor.Metadata.PARAMS, getParamExtractor());
        writeStrictFormatting(list, paramExtractor, SCRIPT);
    }

    /**
     * Script format meant for versions 2.x to 5.x. Required format for 5.x and above.
     * @param list Consumer of snippets
     * @param paramExtractor Extracts parameters from documents or constants
     */
    private void writeStrictFormatting(List<Object> list, Object paramExtractor, String scriptToUse) {
        if (HAS_SCRIPT) {
            /*
             * {
             *   "script":{
             *     "inline": "...",
             *     "lang": "...",
             *     "params": ...,
             *   },
             *   "scripted_upsert":true,
             *   "upsert": {...}
             * }
             */
            list.add(scriptToUse);
            if (HAS_LANG) {
                list.add(SCRIPT_LANG);
            }
            if (paramExtractor != null) {
                list.add(",\"params\":");
                list.add(paramExtractor);
            }
            list.add("}");
            if (HAS_SCRIPT_UPSERT) {
                list.add(",\"scripted_upsert\": true");
            }
            if (UPSERT) {
                list.add(",\"upsert\":");
            }
        } else {
            /*
             * {
             *   "doc_as_upsert": true,
             *   "doc": {...}
             * }
             */
            list.add("{");
            if (UPSERT) {
                list.add("\"doc_as_upsert\":true,");
            }
            list.add("\"doc\":");
        }
    }

    @Override
    protected void writeObjectEnd(List<Object> after) {
        after.add("}");
        super.writeObjectEnd(after);
    }

    @Override
    protected boolean id(List<Object> list, boolean commaMightBeNeeded) {
        boolean added = super.id(list, commaMightBeNeeded);
        Assert.isTrue(added, String.format("Operation [%s] requires an id but none was given/found", getOperation()));
        return added;
    }
}