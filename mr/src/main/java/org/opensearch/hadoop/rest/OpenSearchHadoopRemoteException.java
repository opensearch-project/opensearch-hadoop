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

package org.opensearch.hadoop.rest;

import org.opensearch.hadoop.OpenSearchHadoopException;

public class OpenSearchHadoopRemoteException extends OpenSearchHadoopException {
	private static final long serialVersionUID = 5402297229024034583L;
	
	private String type=null;
	
	public OpenSearchHadoopRemoteException(String message) {
		super(message);
	}
	public OpenSearchHadoopRemoteException(String message, Throwable throwable) {
		super(message, throwable);
	}
	public OpenSearchHadoopRemoteException(String type, String message) {
		super(message);
		this.type = type;
	}
	public OpenSearchHadoopRemoteException(String type, String message, Throwable throwable) {
		super(message, throwable);
		this.type = type;
	}
	
	public String getType() {
		return type;
	}
	
	public String toString() {
        String s = getClass().getName();
        String message = getLocalizedMessage();
        String type = this.getType();
        
        final StringBuilder b = new StringBuilder();
        b.append(s);
        if(type != null) {
        	b.append(": ").append(type);
        }
        if(message != null) {
        	b.append(": ").append(message);
        }
        return b.toString();
    }
}