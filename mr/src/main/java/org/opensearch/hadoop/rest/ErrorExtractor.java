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

import java.util.List;
import java.util.Map;

import org.opensearch.hadoop.OpenSearchHadoopException;

/**
 * Encapsulates logic for parsing and understanding error messages from OpenSearch.
 */
public class ErrorExtractor {

    public ErrorExtractor() {
    }
    
    @SuppressWarnings("rawtypes")
	public OpenSearchHadoopException extractErrorWithCause(Map m) {
    	Object type = m.get("type");
    	Object reason = m.get("reason");
    	Object causedBy = m.get("caused_by");
    	
    	OpenSearchHadoopException ex = null;
    	if(reason != null) {
    		if(type != null) {
    			ex = new OpenSearchHadoopRemoteException(type.toString(), reason.toString());
    		} else {
    			ex = new OpenSearchHadoopRemoteException(reason.toString());
    		}
    	}
    	if(causedBy != null) {
    		if(ex == null) {
    			ex = extractErrorWithCause((Map)causedBy);
    		} else {
    			ex.initCause(extractErrorWithCause((Map)causedBy));
    		}
    	}
    	
    	if(ex == null) {
    		ex = new OpenSearchHadoopRemoteException(m.toString());
    	}
    	
    	return ex;
    }

    @SuppressWarnings("rawtypes")
	public OpenSearchHadoopException extractError(Map jsonMap) {
        Object err = jsonMap.get("error");
        OpenSearchHadoopException error = null;
        if (err != null) {
            // part of ES 2.0
            if (err instanceof Map) {
                Map m = ((Map) err);
                err = m.get("root_cause");
                if (err == null) {
                    error = extractErrorWithCause(m);
                }
                else {
                    if (err instanceof List) {
                        Object nested = ((List) err).get(0);
                        if (nested instanceof Map) {
                            Map nestedM = (Map) nested;
                            if (nestedM.containsKey("reason")) {
                            	error = extractErrorWithCause(nestedM);
                            }
                            else {
                                error = new OpenSearchHadoopRemoteException(nested.toString());
                            }
                        }
                        else {
                        	error = new OpenSearchHadoopRemoteException(nested.toString());
                        }
                    }
                    else {
                    	error = new OpenSearchHadoopRemoteException(err.toString());
                    }
                }
            }
            else {
            	error = new OpenSearchHadoopRemoteException(err.toString());
            }
        }
        return error;
    }
}