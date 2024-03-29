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

package org.opensearch.hadoop.rest.bulk.bwc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.opensearch.hadoop.rest.Resource;
import org.opensearch.hadoop.rest.RestClient;
import org.opensearch.hadoop.rest.bulk.BulkOutputGenerator;
import org.opensearch.hadoop.serialization.ParsingUtils;
import org.opensearch.hadoop.serialization.json.JacksonJsonParser;
import org.opensearch.hadoop.serialization.json.JsonFactory;
import org.opensearch.hadoop.serialization.json.ObjectReader;
import org.opensearch.hadoop.thirdparty.codehaus.jackson.JsonParser;
import org.opensearch.hadoop.thirdparty.codehaus.jackson.map.DeserializationConfig;
import org.opensearch.hadoop.thirdparty.codehaus.jackson.map.ObjectMapper;
import org.opensearch.hadoop.thirdparty.codehaus.jackson.map.SerializationConfig;
import org.opensearch.hadoop.util.Assert;
import org.opensearch.hadoop.util.FastByteArrayInputStream;

public abstract class BulkOutputGeneratorBase implements BulkOutputGenerator {

    private static final String TOOK = "$TOOK$";
    private static final String ERRS = "$ERRS$";
    private static final String OP = "$OP$";
    private static final String IDX = "$IDX$";
    private static final String TYPE = "$TYPE$";
    private static final String ID = "$ID$";
    private static final String VER = "$VER$";
    private static final String STAT = "$STAT$";
    private static final String ETYPE = "$ETYPE$";
    private static final String EMESG = "$EMESG$";

    private final String head =
            "{\n" +
                    "  \"took\": $TOOK$,\n" +
                    "  \"errors\": $ERRS$,\n" +
                    "  \"items\": [\n";

    private final String tail =
            "  ]\n" +
                    "}";

    private Resource resource;
    private long took = 0L;
    private boolean errors = false;
    private List<String> items = new ArrayList<String>();

    private final ObjectMapper mapper;

    public BulkOutputGeneratorBase() {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    protected String getHead() {
        return head;
    }

    protected String getTail() {
        return tail;
    }

    protected abstract String getSuccess();

    protected abstract String getFailure();

    protected abstract Integer getRejectedStatus();

    protected abstract String getRejectionType();

    protected abstract String getRejectionMsg();

    @Override
    public BulkOutputGenerator setInfo(Resource resource, long took) {
        this.resource = resource;
        this.took = took;
        return this;
    }

    @Override
    public BulkOutputGenerator addSuccess(String operation, int status) {
        Assert.notNull(resource);
        items.add(getSuccess()
                .replace(OP, operation)
                .replace(IDX, resource.index())
                .replace(TYPE, resource.type())
                .replace(ID, UUID.randomUUID().toString())
                .replace(VER, "1")
                .replace(STAT, "201")
        );
        return this;
    }

    @Override
    public BulkOutputGenerator addFailure(String operation, int status, String type, String errorMessage) {
        Assert.notNull(resource);
        errors = true;
        items.add(getFailure()
                .replace(OP, operation)
                .replace(IDX, resource.index())
                .replace(TYPE, resource.type())
                .replace(ID, UUID.randomUUID().toString())
                .replace(STAT, Integer.toString(status))
                .replace(ETYPE, type)
                .replace(EMESG, errorMessage)
        );
        return this;
    }

    @Override
    public BulkOutputGenerator addRejection(String operation) {
        Assert.notNull(resource);
        errors = true;
        items.add(getFailure()
                .replace(OP, operation)
                .replace(IDX, resource.index())
                .replace(TYPE, resource.type())
                .replace(ID, UUID.randomUUID().toString())
                .replace(STAT, Integer.toString(getRejectedStatus()))
                .replace(ETYPE, getRejectionType())
                .replace(EMESG, getRejectionMsg())
        );
        return this;
    }

    @Override
    public RestClient.BulkActionResponse generate() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(getHead()
                .replace(TOOK, Long.toString(took))
                .replace(ERRS, Boolean.toString(errors))
        );
        boolean first = true;
        for (String item : items) {
            if (!first) {
                sb.append(",\n");
            }
            sb.append(item);
            first = false;
        }
        sb.append("\n").append(getTail());
        byte[] bytes = sb.toString().getBytes(Charsets.UTF_8);

        Iterator<Map> entries;

        ObjectReader r = JsonFactory.objectReader(mapper, Map.class);
        JsonParser parser = mapper.getJsonFactory().createJsonParser(new FastByteArrayInputStream(bytes));
        if (ParsingUtils.seek(new JacksonJsonParser(parser), "items") == null) {
            entries = Collections.<Map>emptyList().iterator();
        } else {
            entries = r.readValues(parser);
        }

        resource = null;
        took = 0L;
        errors = false;
        items.clear();

        return new RestClient.BulkActionResponse(entries, 200, took);
    }
}