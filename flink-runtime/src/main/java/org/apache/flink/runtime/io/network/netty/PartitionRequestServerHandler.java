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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * PartitionRequestServerHandler 负责处理消费端通过 PartitionRequestClient
 * 发送的 PartitionRequest 和 AddCredit 等请求；
 *
 *
 * 首先，当 NettyServer 接收到 PartitionRequest 消息后，
 * PartitionRequestServerHandler 会创建一个 NetworkSequenceViewReader 对象，
 * 请求创建 ResultSubpartitionView, 并将 NetworkSequenceViewReader 保存在 PartitionRequestQueue 中。
 * PartitionRequestQueue 会持有所有请求消费数据的 RemoteInputChannel 的 ID 和 NetworkSequenceViewReader 之间的映射关系。
 *
 * ResultSubpartitionView 用来消费 ResultSubpartition 中的数据，
 * 并在 ResultSubpartition 中有数据可用时获得提醒；
 *
 *
 * Channel handler to initiate data transfers and dispatch backwards flowing task events. */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

    private final ResultPartitionProvider partitionProvider;

    private final TaskEventPublisher taskEventPublisher;

    private final PartitionRequestQueue outboundQueue;

    PartitionRequestServerHandler(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            PartitionRequestQueue outboundQueue) {

        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {

            // 获取接收到消息的类型
            Class<?> msgClazz = msg.getClass();

            // ----------------------------------------------------------------
            // Intermediate result partition requests
            // ----------------------------------------------------------------
            // 如果是分区请求消息
            if (msgClazz == PartitionRequest.class) {
                //Server 端接收到 client 发送的 PartitionRequest
                PartitionRequest request = (PartitionRequest) msg;

                LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

                try {
                    NetworkSequenceViewReader reader;

                    // 创建一个reader
                    reader =
                            new CreditBasedSequenceNumberingViewReader(
                                    request.receiverId, request.credit, outboundQueue);

                    // 为该reader分配一个subpartitionView
                    // 通过 ResultPartitionProvider（实际上就是 ResultPartitionManager）创建 ResultSubpartitionView
                    // 在有可被消费的数据产生后，
                    // PartitionRequestQueue.notifyReaderNonEmpty 会被回调，
                    // 进而在 netty channelPipeline 上触发一次 fireUserEventTriggered
                    reader.requestSubpartitionView(
                            partitionProvider, request.partitionId, request.queueIndex);

                    // 注册reader到outboundQueue中
                    // outboundQueue中存放了多个reader，这些reader在队列中排队，等待数据发送

                    //通知 PartitionRequestQueue 创建了一个 NetworkSequenceViewReader
                    outboundQueue.notifyReaderCreated(reader);
                } catch (PartitionNotFoundException notFound) {
                    respondWithError(ctx, notFound, request.receiverId);
                }
            }
            // ----------------------------------------------------------------
            // Task events
            // ----------------------------------------------------------------
            else if (msgClazz == TaskEventRequest.class) {
                TaskEventRequest request = (TaskEventRequest) msg;

                if (!taskEventPublisher.publish(request.partitionId, request.event)) {
                    respondWithError(
                            ctx,
                            new IllegalArgumentException("Task event receiver not found."),
                            request.receiverId);
                }
            } else if (msgClazz == CancelPartitionRequest.class) {
                CancelPartitionRequest request = (CancelPartitionRequest) msg;

                outboundQueue.cancel(request.receiverId);
            } else if (msgClazz == CloseRequest.class) {
                outboundQueue.close();
            } else if (msgClazz == AddCredit.class) {
                //增加 credit
                AddCredit request = (AddCredit) msg;

                // 调用addCredit方法
                outboundQueue.addCreditOrResumeConsumption(
                        request.receiverId, reader -> reader.addCredit(request.credit));
            } else if (msgClazz == ResumeConsumption.class) {
                ResumeConsumption request = (ResumeConsumption) msg;

                // 调用addCredit方法
                outboundQueue.addCreditOrResumeConsumption(
                        request.receiverId, NetworkSequenceViewReader::resumeConsumption);
            } else {
                LOG.warn("Received unexpected client request: {}", msg);
            }
        } catch (Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
    }

    private void respondWithError(
            ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
        LOG.debug("Responding with error: {}.", error.getClass());

        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
    }
}
