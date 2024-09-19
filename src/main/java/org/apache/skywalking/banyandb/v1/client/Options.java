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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.banyandb.v1.client.grpc.channel.ChannelManagerSettings;

/**
 * Client connection options.
 */
@Setter(AccessLevel.PUBLIC)
@Getter(AccessLevel.PUBLIC)
public class Options {
    /**
     * Max inbound message size
     */
    private int maxInboundMessageSize = 1024 * 1024 * 50;
    /**
     * Threshold of gRPC blocking query, unit is second
     */
    private int deadline = 30;
    /**
     * Refresh interval for the gRPC channel, unit is second
     */
    private long refreshInterval = 30;
    /**
     * Threshold of force gRPC reconnection if network issue is encountered
     */
    private long forceReconnectionThreshold = 1;

    /**
     * Force use TLS for gRPC
     * Default is false
     */
    private boolean forceTLS = false;
    /**
     * SSL: Trusted CA Path
     */
    private String sslTrustCAPath = "";
    /**
     * SSL: Cert Chain Path, BanyanDB server not support mTLS yet
     */
    private String sslCertChainPath = "";
    /**
     * SSL: Cert Key Path, BanyanDB server not support mTLS yet
     */
    private String sslKeyPath = "";

    public Options() {
    }

    ChannelManagerSettings buildChannelManagerSettings() {
        return ChannelManagerSettings.newBuilder()
                .setRefreshInterval(this.refreshInterval)
                .setForceReconnectionThreshold(this.forceReconnectionThreshold)
                .build();
    }
}
