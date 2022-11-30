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

import java.util.EnumMap;

import org.opensearch.hadoop.OpenSearchHadoopUnsupportedOperationException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.SettingsAware;
import org.opensearch.hadoop.serialization.field.FieldExtractor;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;

// specific implementation that relies on basic field extractors that are computed
// lazy per entity. Both the pool and extractors are meant to be reused.
public abstract class PerEntityPoolingMetadataExtractor implements MetadataExtractor, SettingsAware {
    protected OpenSearchMajorVersion version;
    protected Object entity;
    protected Settings settings;

    public static class StaticFieldExtractor implements FieldExtractor {
        private Object field;
        private boolean needsInit = true;

        @Override
        public Object field(Object target) {
            return field;
        }

        protected Object field() {
            return field;
        }

        public void setField(Object field) {
            this.field = field;
            this.needsInit = true;
        }

        public boolean needsInit() {
            return needsInit;
        }
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
        this.version = settings.getInternalVersionOrThrow();
    }

    /**
     * A special field extractor meant to be used for metadata fields that are
     * supported in
     * some versions of Elasticsearch, but not others. In the case that a metadata
     * field is
     * unsupported for the configured version of Elasticsearch, this extractor which
     * throws
     * exceptions for using unsupported metadata tags is returned instead of the
     * regular one.
     */
    private static class UnsupportedMetadataFieldExtractor extends StaticFieldExtractor {
        private Metadata unsupportedMetadata;
        private OpenSearchMajorVersion version;

        public UnsupportedMetadataFieldExtractor(Metadata unsupportedMetadata, OpenSearchMajorVersion version) {
            this.unsupportedMetadata = unsupportedMetadata;
            this.version = version;
        }

        @Override
        public Object field(Object target) {
            throw new OpenSearchHadoopUnsupportedOperationException("Unsupported metadata tag [" + unsupportedMetadata.getName()
                    + "] for OpenSearch version [" + version.toString() + "]. Bailing out...");
        }
    }

    private final EnumMap<Metadata, FieldExtractor> pool = new EnumMap<Metadata, FieldExtractor>(Metadata.class);

    public void reset() {
        entity = null;
    }

    @Override
    public FieldExtractor get(Metadata metadata) {
        FieldExtractor fieldExtractor = pool.get(metadata);

        if (fieldExtractor == null || (fieldExtractor instanceof StaticFieldExtractor
                && ((StaticFieldExtractor) fieldExtractor).needsInit())) {
            Object value = getValue(metadata);
            if (value == null) {
                return null;
            }

            if (fieldExtractor == null) {
                fieldExtractor = _createExtractorFor(metadata);
            }

            if (fieldExtractor instanceof StaticFieldExtractor && ((StaticFieldExtractor) fieldExtractor).needsInit()) {
                ((StaticFieldExtractor) fieldExtractor).setField(value);
            }
            pool.put(metadata, fieldExtractor);
        }
        return fieldExtractor;
    }

    /**
     * If a metadata tag is unsupported for this version of Elasticsearch then a
     */
    private FieldExtractor _createExtractorFor(Metadata metadata) {
        //TTL and Timestamp metadata on index and update requests is not supported.
        switch (metadata) {
            case TTL: // Fall through
            case TIMESTAMP:
                return new UnsupportedMetadataFieldExtractor(metadata, version);
        }
        return createExtractorFor(metadata);
    }

    protected FieldExtractor createExtractorFor(Metadata metadata) {
        return new StaticFieldExtractor();
    }

    public abstract Object getValue(Metadata metadata);

    @Override
    public void setObject(Object entity) {
        this.entity = entity;
    }
}