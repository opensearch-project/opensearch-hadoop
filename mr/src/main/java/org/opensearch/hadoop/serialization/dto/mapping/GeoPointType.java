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
package org.opensearch.hadoop.serialization.dto.mapping;

import org.opensearch.hadoop.serialization.FieldType;

import static org.opensearch.hadoop.serialization.FieldType.*;


public enum GeoPointType implements GeoField {
    LAT_LON_OBJECT(OBJECT, 0),
    LAT_LON_STRING(STRING, 0),
    GEOHASH(STRING, 0),
    LON_LAT_ARRAY(DOUBLE, 1);

    private final FieldType primitiveType;
    private final int arrayDepth;
    
    GeoPointType(FieldType primitiveType, int arrayDepth) {
        this.primitiveType = primitiveType;
        this.arrayDepth = arrayDepth;
    }
    
    @Override
    public FieldType rawType() {
        return primitiveType;
    }

    @Override
    public int arrayDepth() {
        return arrayDepth;
    }
}