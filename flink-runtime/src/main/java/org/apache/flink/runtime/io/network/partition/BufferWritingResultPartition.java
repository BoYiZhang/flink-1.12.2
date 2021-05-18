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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;


import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
public abstract class BufferWritingResultPartition extends ResultPartition {



    // ResultPartition 由 ResultSubpartition 构成，
    // ResultSubpartition 的数量由下游消费 Task 数和 DistributionPattern 来决定。
    // 例如，如果是 FORWARD，则下游只有一个消费者；如果是 SHUFFLE，则下游消费者的数量和下游算子的并行度一样

    /** The subpartitions of this partition. At least one. */
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    private final BufferBuilder[] unicastBufferBuilders;

    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    private BufferBuilder broadcastBufferBuilder;

    private Meter idleTimeMsPerSecond = new MeterView(new SimpleCounter());

    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.subpartitions = checkNotNull(subpartitions);
        // 根据子分区的数量构建
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    public void setup() throws IOException {
        super.setup();

        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    //将序列化结果写入buffer
    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {

        //    record = {HeapByteBuffer@7875} "java.nio.HeapByteBuffer[pos=0 lim=15 cap=128]"
        //    targetSubpartition = 2

        // 1. 获取一个BufferBuilder
        // 2. 写入数据流元素到bufferBuilder
        // 3. 如果buffer被写满，修改buffer状态为finished，申请新的buffer继续写入
        // 4. buffer写满同时元素也被完全写入，需要跳出循环 ???



        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);

        //当前这条记录没有写完，申请新的 buffer 写入
        while (record.hasRemaining()) {

            // 这里很重要，当一个buffer被写满的时候需要标记该buffer的状态为finished
            // 在ResultSubpartition取出缓存数据的时候会对buffer队列中每个buffer的finished状态进行检查
            // 只允许buffer队列中最后一个buffer的状态为没有finished

            // full buffer, partial record
            //buffer 写满了，调用 finishUnicastBufferBuilder 方法
            finishUnicastBufferBuilder(targetSubpartition);

            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            //buffer 写满了，调用 finishUnicastBufferBuilder 方法
            finishUnicastBufferBuilder(targetSubpartition);
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each channel of targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        idleTimeMsPerSecond = metrics.getIdleTimeMsPerSecond();
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");

        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {

        //    unicastBufferBuilders = {BufferBuilder[4]@7861}
        //        0 = {BufferBuilder@7882}
        //        1 = {BufferBuilder@7883}
        //        2 = {BufferBuilder@7878}
        //        3 = {BufferBuilder@7884}
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == null) {
            // 如果 buffer 没有创建 , 构建一个
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            // 构建 BufferConsumer
            subpartitions[targetSubpartition].add(buffer.createBufferConsumerFromBeginning(), 0);
        }
        // 继续写记录
        buffer.appendAndCommit(record);

        return buffer;
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        // 请求一个新的 BufferBuilder
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);


        // 注意，如果是partialRecordBytes！=0，
        // 部分长度和数据必须先 “appendAndCommit”，然后才能创建 consumer。

        // 否则会与缓冲区以完整记录开始的情况相混淆。
        // 下两行不能改变顺序。

        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created.
        //
        // Otherwise it would be confused with the case the buffer starting with a complete record.
        // !! The next two lines can not change order.


        // 提交...
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);

        // 设置消费者
        subpartitions[targetSubpartition].add(

                buffer.createBufferConsumerFromBeginning(), partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0);
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        createBroadcastBufferConsumers(buffer, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(BufferBuilder buffer, int partialRecordBytes)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            for (ResultSubpartition subpartition : subpartitions) {
                subpartition.add(consumer.copy(), partialRecordBytes);
            }
        }
    }

    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        checkInProduceState();
        ensureUnicastMode();
        // 请求新的BufferBuilder

        //请求新的 BufferBuilder，用于写入数据 如果当前没有可用的 buffer，会阻塞
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);

        // 设置值
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {


        // 从 LocalBufferPool 申请BufferBuilder
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        if (bufferBuilder != null) {
            return bufferBuilder;
        }

        final long start = System.currentTimeMillis();
        try {
            // 因为从 LocalBufferPool的缓存中没有申请到内存, 开始进入阻塞模式,等待有新的内存释放
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            idleTimeMsPerSecond.markEvent(System.currentTimeMillis() - start);
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            // 数据容量增加
            numBytesOut.inc(bufferBuilder.finish());
            //条数 +1
            numBuffersOut.inc();

            // 设置为null ??
            unicastBufferBuilders[targetSubpartition] = null;
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
            // buffer 写满了，调用 finishUnicastBufferBuilder 方法
            finishUnicastBufferBuilder(channelIndex);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            numBytesOut.inc(broadcastBufferBuilder.finish() * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public Meter getIdleTimeMsPerSecond() {
        return idleTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}
