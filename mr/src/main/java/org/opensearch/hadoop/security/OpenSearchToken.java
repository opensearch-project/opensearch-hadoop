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

package org.opensearch.hadoop.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.opensearch.hadoop.util.OpenSearchMajorVersion;

/**
 * Stores token authentication information for an OpenSearch user.
 */
public class OpenSearchToken {

    private final String name;
    private final String id;
    private final String apiKey;
    private final long expirationTime;
    private final String clusterName;
    private final OpenSearchMajorVersion majorVersion;

    public OpenSearchToken(String name, String id, String apiKey, long expirationTime, String clusterName, OpenSearchMajorVersion majorVersion) {
        this.name = name;
        this.id = id;
        this.apiKey = apiKey;
        this.expirationTime = expirationTime;
        this.clusterName = clusterName;
        this.majorVersion = majorVersion;
    }

    public OpenSearchToken(DataInput inputStream) throws IOException {
        this.name = inputStream.readUTF();
        this.id = inputStream.readUTF();
        this.apiKey = inputStream.readUTF();
        this.expirationTime = inputStream.readLong();
        this.clusterName = inputStream.readUTF();
        this.majorVersion = OpenSearchMajorVersion.parse(inputStream.readUTF());
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public String getApiKey() {
        return apiKey;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public String getClusterName() {
        return clusterName;
    }

    public OpenSearchMajorVersion getMajorVersion() {
        return majorVersion;
    }

    public void writeOut(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(apiKey);
        dataOutput.writeLong(expirationTime);
        dataOutput.writeUTF(clusterName);
        dataOutput.writeUTF(majorVersion.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OpenSearchToken opensearchToken = (OpenSearchToken) o;

        if (expirationTime != opensearchToken.expirationTime) return false;
        if (name != null ? !name.equals(opensearchToken.name) : opensearchToken.name != null) return false;
        if (id != null ? !id.equals(opensearchToken.id) : opensearchToken.id != null) return false;
        if (apiKey != null ? !apiKey.equals(opensearchToken.apiKey) : opensearchToken.apiKey != null) return false;
        if (clusterName != null ? !clusterName.equals(opensearchToken.clusterName) : opensearchToken.clusterName != null) return false;
        return majorVersion != null ? majorVersion.equals(opensearchToken.majorVersion) : opensearchToken.majorVersion == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (apiKey != null ? apiKey.hashCode() : 0);
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
        result = 31 * result + (majorVersion != null ? majorVersion.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "OpenSearchToken{" +
                "name='" + name + '\'' +
                ", id='" + id + '\'' +
                ", apiKey='" + apiKey + '\'' +
                ", expirationTime=" + expirationTime +
                ", clusterName='" + clusterName + '\'' +
                ", majorVersion=" + majorVersion +
                '}';
    }
}