/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.banyandb.v1.client;

import org.apache.skywalking.banyandb.trace.v1.BanyandbTrace;

import java.util.List;

/**
 * TraceQueryResponse is a high-level response object for the trace query API.
 */
public class TraceQueryResponse {
    private final BanyandbTrace.QueryResponse response;

    TraceQueryResponse(BanyandbTrace.QueryResponse response) {
        this.response = response;
    }

    /**
     * Get the list of traces returned by the query.
     *
     * @return list of traces, each containing spans grouped by trace ID
     */
    public List<BanyandbTrace.Trace> getTraces() {
        return response.getTracesList();
    }

    /**
     * Get the trace query execution trace if enabled.
     *
     * @return trace query execution trace or null if not enabled
     */
    public String getTraceResult() {
        if (response.hasTraceQueryResult()) {
            return response.getTraceQueryResult().toString();
        }
        return null;
    }

    /**
     * Get the total number of traces returned.
     *
     * @return trace count
     */
    public int size() {
        return response.getTracesCount();
    }

    /**
     * Check if the response is empty.
     *
     * @return true if no traces were returned
     */
    public boolean isEmpty() {
        return size() == 0;
    }
}