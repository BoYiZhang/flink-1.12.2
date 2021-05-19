/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * NettyConnectionManager 在启动的时候会创建并启动 NettyClient 和 NettyServer，
 * NettyServer 会启动一个服务端监听，等待其它 NettyClient 的连接：
 *
 *
 * 当 RemoteInputChannel 请求一个远端的 ResultSubpartition 的时候，
 * NettyClient 就会发起和请求的 ResultSubpartition 所在 Task 的 NettyServer 的连接，
 * 后续所有的数据交换都在这个连接上进行。
 *
 * 两个 Task 之间只会建立一个连接，
 * 这个连接会在不同的 RemoteInputChannel 和 ResultSubpartition 之间进行复用
 *
 */
public class NettyConnectionManager implements ConnectionManager {

    private final NettyServer server;

    private final NettyClient client;

    private final NettyBufferPool bufferPool;

    private final PartitionRequestClientFactory partitionRequestClientFactory;

    private final NettyProtocol nettyProtocol;

    public NettyConnectionManager(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig) {

        this.server = new NettyServer(nettyConfig);
        this.client = new NettyClient(nettyConfig);
        this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

        this.partitionRequestClientFactory =
                new PartitionRequestClientFactory(client, nettyConfig.getNetworkRetries());

        this.nettyProtocol =
                new NettyProtocol(
                        checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));
    }

    @Override
    public int start() throws IOException {
        //初始化 Netty Client
        client.init(nettyProtocol, bufferPool);


        // 初始化并启动 Netty Server
        return server.init(nettyProtocol, bufferPool);
    }

    @Override
    public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        //这里实际上会建立和其它 Task 的 Server 的连接
        //返回的 PartitionRequestClient 中封装了 netty channel 和 channel handler
        return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
    }

    @Override
    public void closeOpenChannelConnections(ConnectionID connectionId) {
        partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
    }

    @Override
    public int getNumberOfActiveConnections() {
        return partitionRequestClientFactory.getNumberOfActiveClients();
    }

    @Override
    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }

    NettyClient getClient() {
        return client;
    }

    NettyServer getServer() {
        return server;
    }

    NettyBufferPool getBufferPool() {
        return bufferPool;
    }
}
