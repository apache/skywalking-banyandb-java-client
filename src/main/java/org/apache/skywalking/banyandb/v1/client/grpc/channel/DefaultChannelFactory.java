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

package org.apache.skywalking.banyandb.v1.client.grpc.channel;

import com.google.common.base.Strings;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.banyandb.v1.client.Options;
import org.apache.skywalking.banyandb.v1.client.util.PrivateKeyUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

@Slf4j
@RequiredArgsConstructor
public class DefaultChannelFactory implements ChannelFactory {
    private final String host;
    private final int port;
    private final Options options;

    @Override
    public ManagedChannel create() throws IOException {
        NettyChannelBuilder managedChannelBuilder = NettyChannelBuilder.forAddress(this.host, this.port)
                .maxInboundMessageSize(options.getMaxInboundMessageSize())
                .usePlaintext();

        File caFile = new File(options.getSslTrustCAPath());
        boolean isCAFileExist = caFile.exists() && caFile.isFile();
        if (options.isForceTLS() || isCAFileExist) {
            SslContextBuilder builder = GrpcSslContexts.forClient();

            if (isCAFileExist) {
                String certPath = options.getSslCertChainPath();
                String keyPath = options.getSslKeyPath();
                if (!Strings.isNullOrEmpty(certPath) && Strings.isNullOrEmpty(keyPath)) {
                    File keyFile = new File(keyPath);
                    File certFile = new File(certPath);

                    if (certFile.isFile() && keyFile.isFile()) {
                        try (InputStream cert = new FileInputStream(certFile);
                             InputStream key = PrivateKeyUtil.loadDecryptionKey(keyFile.getAbsolutePath())) {
                            builder.keyManager(cert, key);
                        }
                    } else if (!certFile.isFile() || !keyFile.isFile()) {
                        log.warn("Failed to enable mTLS caused by cert or key cannot be found.");
                    }
                }

                builder.trustManager(caFile);
            }
            managedChannelBuilder.negotiationType(NegotiationType.TLS).sslContext(builder.build());
        }
        return managedChannelBuilder.build();
    }
}
