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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link ResultSubpartition#add(BufferConsumer)} adds a finished {@link BufferConsumer}
 * or a second {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via {@link
 * ResultSubpartition#createReadView(BufferAvailabilityListener)} of new data availability. Except
 * by calling {@link #flush()} explicitly, we always only notify when the first finished buffer
 * turns up and then, the reader has to drain the buffers via {@link #pollBuffer()} until its return
 * value shows no more buffers being available. This results in a buffer queue which is either empty
 * or has an unfinished {@link BufferConsumer} left from which the notifications will eventually
 * start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this {@link
 * PipelinedSubpartitionView#notifyDataAvailable() notification} for any {@link BufferConsumer}
 * present in the queue.
 */
public class PipelinedSubpartition extends ResultSubpartition
        implements CheckpointedResultSubpartition, ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

    // ------------------------------------------------------------------------

    /**
     *
     * 写入的 Buffer 最终被保存在 ResultSubpartition 中维护的一个队列中，
     * 如果需要消费这些 Buffer，就需要依赖 ResultSubpartitionView。
     *
     * 当需要消费一个 ResultSubpartition 的结果时，需要创建一个 ResultSubpartitionView 对象，
     * 并关联到 ResultSubpartition 中；当数据可以被消费时，
     * 会通过对应的回调接口告知 ResultSubpartitionView：
     *
     *
     * 当前 subpartiion 堆积的所有的 Buffer 的队列
     *
     * All buffers of this subpartition.
     * Access to the buffers is synchronized on this object.
     *
     * */
    final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers =  new PrioritizedDeque<>();

    /**
     * //当前 subpartiion 中堆积的 buffer 的数量
     * The number of non-event buffers currently in this subpartition.
     * */
    @GuardedBy("buffers")
    private int buffersInBacklog;

    /**
     * 用于消费写入的 Buffer
     * The read view to consume this subpartition.
     * */
    PipelinedSubpartitionView readView;

    /** Flag indicating whether the subpartition has been finished. */
    private boolean isFinished;

    @GuardedBy("buffers")
    private boolean flushRequested;

    /** Flag indicating whether the subpartition has been released. */
    volatile boolean isReleased;

    /** The total number of buffers (both data and event buffers). */
    private long totalNumberOfBuffers;

    /** The total number of bytes (both data and event buffers). */
    private long totalNumberOfBytes;

    /** Writes in-flight data. */
    private ChannelStateWriter channelStateWriter;

    /**
     * Whether this subpartition is blocked (e.g. by exactly once checkpoint) and is waiting for
     * resumption.
     */
    @GuardedBy("buffers")
    boolean isBlocked = false;

    int sequenceNumber = 0;

    // ------------------------------------------------------------------------

    // index 是当前 sub-paritition 的索引
    PipelinedSubpartition(int index, ResultPartition parent) {
        super(index, parent);
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    @Override
    public boolean add(BufferConsumer bufferConsumer, int partialRecordLength) {
        return add(bufferConsumer, partialRecordLength, false);
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        if (notifyAndBlockOnCompletion) {
            add(EventSerializer.toBufferConsumer(EndOfChannelStateEvent.INSTANCE, false), 0, false);
        }
    }

    @Override
    public void finish() throws IOException {
        add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false), 0, true);
        LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
    }

    // 添加一个新的BufferConsumer
    // 这个参数里的 finish 指的是整个 subpartition 都完成了
    private boolean add(BufferConsumer bufferConsumer, int partialRecordLength, boolean finish) {
        checkNotNull(bufferConsumer);

        final boolean notifyDataAvailable;
        int prioritySequenceNumber = -1;

        // 同步队列 ???
        synchronized (buffers) {

            if (isFinished || isReleased) {
                bufferConsumer.close();
                return false;
            }

            // 添加bufferConsumer & 更新状态
            // Add the bufferConsumer and update the stats
            if (addBuffer(bufferConsumer, partialRecordLength)) {
                prioritySequenceNumber = sequenceNumber;
            }
            updateStatistics(bufferConsumer);

            //更新 backlog 的数量，只有 buffer 才会使得 buffersInBacklog + 1，事件不会增加 buffersInBacklog
            increaseBuffersInBacklog(bufferConsumer);
            notifyDataAvailable = finish || shouldNotifyDataAvailable();

            isFinished |= finish;
        }

        if (prioritySequenceNumber != -1) {
            notifyPriorityEvent(prioritySequenceNumber);
        }
        if (notifyDataAvailable) {
            //通知数据可以被消费
            notifyDataAvailable();
        }

        return true;
    }

    private boolean addBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        assert Thread.holdsLock(buffers);
        if (bufferConsumer.getDataType().hasPriority()) {
            return processPriorityBuffer(bufferConsumer, partialRecordLength);
        }
        // 添加消费者 ???
        buffers.add(new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        return false;
    }

    private boolean processPriorityBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        final int numPriorityElements = buffers.getNumPriorityElements();

        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        if (barrier != null) {
            checkState(
                    barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                    "Only unaligned checkpoints should be priority events");
            final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
            Iterators.advance(iterator, numPriorityElements);
            List<Buffer> inflightBuffers = new ArrayList<>();
            while (iterator.hasNext()) {
                BufferConsumer buffer = iterator.next().getBufferConsumer();

                if (buffer.isBuffer()) {
                    try (BufferConsumer bc = buffer.copy()) {
                        inflightBuffers.add(bc.build());
                    }
                }
            }
            if (!inflightBuffers.isEmpty()) {
                channelStateWriter.addOutputData(
                        barrier.getId(),
                        subpartitionInfo,
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        inflightBuffers.toArray(new Buffer[0]));
            }
        }
        return numPriorityElements == 1
                && !isBlocked; // if subpartition is blocked then downstream doesn't expect any
        // notifications
    }

    @Nullable
    private CheckpointBarrier parseCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier;
        try (BufferConsumer bc = bufferConsumer.copy()) {
            Buffer buffer = bc.build();
            try {
                final AbstractEvent event =
                        EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
                barrier = event instanceof CheckpointBarrier ? (CheckpointBarrier) event : null;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Should always be able to deserialize in-memory event", e);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return barrier;
    }

    @Override
    public void release() {
        // view reference accessible outside the lock, but assigned inside the locked scope
        final PipelinedSubpartitionView view;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            for (BufferConsumerWithPartialRecordLength buffer : buffers) {
                buffer.getBufferConsumer().close();
            }
            buffers.clear();

            view = readView;
            readView = null;

            // Make sure that no further buffers are added to the subpartition
            isReleased = true;
        }

        LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

        if (view != null) {
            view.releaseAllResources();
        }
    }

    @Nullable
    BufferAndBacklog pollBuffer() {
        synchronized (buffers) {
            if (isBlocked) {
                return null;
            }

            Buffer buffer = null;

            // 如果buffers队列为空，不需要flush
            if (buffers.isEmpty()) {
                flushRequested = false;
            }

            while (!buffers.isEmpty()) {

                BufferConsumerWithPartialRecordLength bufferConsumerWithPartialRecordLength = buffers.peek();


                BufferConsumer bufferConsumer =
                        bufferConsumerWithPartialRecordLength.getBufferConsumer();

               // 转换bufferConsumer为Buffer类型
                buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);

                // 检查buffers队列，没有finished的buffer不能在队列头部
                // 只允许buffer队列最后一个buffer的状态为没有finish
                checkState(
                        bufferConsumer.isFinished() || buffers.size() == 1,
                        "When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

                // 如果队列只有一个buffer，不需要flush
                if (buffers.size() == 1) {
                    // turn off flushRequested flag if we drained all of the available data
                    flushRequested = false;
                }

                // 如果当前bufferConsumer已经finish，回收这个buffer，待处理的buffer数量减1
                if (bufferConsumer.isFinished()) {
                    // 注意，回收已经finished的buffer的调用在此
                    requireNonNull(buffers.poll()).getBufferConsumer().close();
                    decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
                }

                // 如果可读字节数大于0，跳出循环
                if (buffer.readableBytes() > 0) {
                    break;
                }
                // 否则回收内存
                buffer.recycleBuffer();
                buffer = null;
                if (!bufferConsumer.isFinished()) {
                    // 运行到这里只有一种可能，buffer队列中只有一个没有finished的buffer，跳出循环
                    break;
                }
            }

            if (buffer == null) {
                return null;
            }

            if (buffer.getDataType().isBlockingUpstream()) {
                isBlocked = true;
            }

            // 更新总字节数计数器
            updateStatistics(buffer);


            // Do not report last remaining buffer on buffers as available to read (assuming it's
            // unfinished).
            // It will be reported for reading either on flush or when the number of buffers in the
            // queue
            // will be 2 or more.
            NetworkActionsLogger.traceOutput(
                    "PipelinedSubpartition#pollBuffer",
                    buffer,
                    parent.getOwningTaskName(),
                    subpartitionInfo);

            // 返回BufferAndBacklog对象，该对象包装了包含数据的buffer
            return new BufferAndBacklog(
                    buffer,
                    getBuffersInBacklog(),
                    isDataAvailableUnsafe() ? getNextBufferTypeUnsafe() : Buffer.DataType.NONE,
                    sequenceNumber++);
        }
    }

    void resumeConsumption() {
        synchronized (buffers) {
            checkState(isBlocked, "Should be blocked by checkpoint.");

            isBlocked = false;
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public PipelinedSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) {
        synchronized (buffers) {
            // 检查该SubPartition的缓存不能被释放
            checkState(!isReleased);

            // 检查之前不能创建过read view
            checkState(
                    readView == null,
                    "Subpartition %s of is being (or already has been) consumed, "
                            + "but pipelined subpartitions can only be consumed once.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());
            // 创建view，同时转入availabilityListener
            readView = new PipelinedSubpartitionView(this, availabilityListener);
        }

        return readView;
    }

    public boolean isAvailable(int numCreditsAvailable) {
        synchronized (buffers) {
            if (numCreditsAvailable > 0) {
                return isDataAvailableUnsafe();
            }

            final Buffer.DataType dataType = getNextBufferTypeUnsafe();
            return dataType.isEvent();
        }
    }

    @GuardedBy("buffers")
    private boolean isDataAvailableUnsafe() {
        assert Thread.holdsLock(buffers);

        return !isBlocked && (flushRequested || getNumberOfFinishedBuffers() > 0);
    }

    private Buffer.DataType getNextBufferTypeUnsafe() {
        assert Thread.holdsLock(buffers);

        final BufferConsumerWithPartialRecordLength first = buffers.peek();
        return first != null ? first.getBufferConsumer().getDataType() : Buffer.DataType.NONE;
    }

    // ------------------------------------------------------------------------

    int getCurrentNumberOfBuffers() {
        return buffers.size();
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final long numBuffers;
        final long numBytes;
        final boolean finished;
        final boolean hasReadView;

        synchronized (buffers) {
            numBuffers = getTotalNumberOfBuffers();
            numBytes = getTotalNumberOfBytes();
            finished = isFinished;
            hasReadView = readView != null;
        }

        return String.format(
                "%s#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
                this.getClass().getSimpleName(),
                getSubPartitionIndex(),
                numBuffers,
                numBytes,
                getBuffersInBacklog(),
                finished,
                hasReadView);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        // since we do not synchronize, the size may actually be lower than 0!
        return Math.max(buffers.size(), 0);
    }

    @Override
    public void flush() {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            if (buffers.isEmpty() || flushRequested) {
                return;
            }
            // if there is more then 1 buffer, we already notified the reader
            // (at the latest when adding the second buffer)
            notifyDataAvailable =
                    !isBlocked
                            && buffers.size() == 1
                            && buffers.peek().getBufferConsumer().isDataAvailable();
            flushRequested = buffers.size() > 1 || notifyDataAvailable;
        }
        if (notifyDataAvailable) {
            notifyDataAvailable();
        }
    }

    @Override
    protected long getTotalNumberOfBuffers() {
        return totalNumberOfBuffers;
    }

    @Override
    protected long getTotalNumberOfBytes() {
        return totalNumberOfBytes;
    }

    Throwable getFailureCause() {
        return parent.getFailureCause();
    }

    private void updateStatistics(BufferConsumer buffer) {
        totalNumberOfBuffers++;
    }

    private void updateStatistics(Buffer buffer) {
        totalNumberOfBytes += buffer.getSize();
    }

    @GuardedBy("buffers")
    private void decreaseBuffersInBacklogUnsafe(boolean isBuffer) {
        assert Thread.holdsLock(buffers);
        if (isBuffer) {
            buffersInBacklog--;
        }
    }

    /**
     * Increases the number of non-event buffers by one after adding a non-event buffer into this
     * subpartition.
     */
    @GuardedBy("buffers")
    private void increaseBuffersInBacklog(BufferConsumer buffer) {
        assert Thread.holdsLock(buffers);

        if (buffer != null && buffer.isBuffer()) {
            buffersInBacklog++;
        }
    }

    /**
     * Gets the number of non-event buffers in this subpartition.
     *
     * <p><strong>Beware:</strong> This method should only be used in tests in non-concurrent access
     * scenarios since it does not make any concurrency guarantees.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    public int getBuffersInBacklog() {
        if (flushRequested || isFinished) {
            return buffersInBacklog;
        } else {
            return Math.max(buffersInBacklog - 1, 0);
        }
    }

    //只在第一个 buffer 为 finish 的时候才通知
    @GuardedBy("buffers")
    private boolean shouldNotifyDataAvailable() {
        // Notify only when we added first finished buffer.
        return readView != null
                && !flushRequested
                && !isBlocked
                && getNumberOfFinishedBuffers() == 1;
    }

    //通知readView，有数据可用了
    private void notifyDataAvailable() {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null) {
            readView.notifyDataAvailable();
        }
    }

    //通知readView，有 EVENT 可用了
    private void notifyPriorityEvent(int prioritySequenceNumber) {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null) {
            readView.notifyPriorityEvent(prioritySequenceNumber);
        }
    }

    private int getNumberOfFinishedBuffers() {
        assert Thread.holdsLock(buffers);

        // 注意：isFinished（）不能保证在这里提供最新的状态
        // 最坏的情况是：在下一个flush（）调用之前，只有一个完成的缓冲区
        // （但我们不会提供更有力的担保）

        // NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
        // worst-case: a single finished buffer sits around until the next flush() call
        // (but we do not offer stronger guarantees anyway)
        final int numBuffers = buffers.size();
        if (numBuffers == 1 && buffers.peekLast().getBufferConsumer().isFinished()) {
            return 1;
        }

        // 我们假设只有最后一个缓冲区没有完成。
        // We assume that only last buffer is not finished.
        return Math.max(0, numBuffers - 1);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return parent.getBufferPool().requestBufferBuilderBlocking();
    }

    Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
        return buffer.build();
    }

    /** for testing only. */
    @VisibleForTesting
    BufferConsumerWithPartialRecordLength getNextBuffer() {
        return buffers.poll();
    }
}
