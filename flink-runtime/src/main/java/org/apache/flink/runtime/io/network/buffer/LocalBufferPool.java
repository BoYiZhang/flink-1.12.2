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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.concurrent.FutureUtils.assertNoException;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 *
 * LocalBufferPool是NetworkBufferPool的包装。
 *
 * 负责分配和回收NetworkBufferPool中的一部分buffer对象。
 *
 * NetworkBufferPool是一个固定大小的缓存池。
 *
 * 将一个NetworkBufferPool的可用缓存划分给多个LocalBufferPool使用，
 *
 * 避免网络层同时操作NetworkBufferPool造成死锁。
 *
 * 同时LocalBufferPool实现了默认的回收机制，确保每一个buffer最终会返回给NetworkBufferPool。

 * A buffer pool used to manage a number of {@link Buffer} instances from the {@link
 * NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure dead-lock free operation of
 * the network stack by limiting the number of buffers per local buffer pool. It also implements the
 * default mechanism for buffer recycling, which ensures that every buffer is ultimately returned to
 * the network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to match
 * its new size.
 *
 * <p>Availability is defined as returning a segment on a subsequent {@link #requestBuffer()}/
 * {@link #requestBufferBuilder()} and heaving a non-blocking {@link
 * #requestBufferBuilderBlocking(int)}. In particular,
 *
 * <ul>
 *   <li>There is at least one {@link #availableMemorySegments}.
 *   <li>No subpartitions has reached {@link #maxBuffersPerChannel}.
 * </ul>
 *
 * <p>To ensure this contract, the implementation eagerly fetches additional memory segments from
 * {@link NetworkBufferPool} as long as it hasn't reached {@link #maxNumberOfMemorySegments} or one
 * subpartition reached the quota.
 */
class LocalBufferPool implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

    private static final int UNKNOWN_CHANNEL = -1;

    /**
     * buffers 从全局 network buffer pool 获取
     *
     * Global network buffer pool to get buffers from. */
    private final NetworkBufferPool networkBufferPool;

    /**
     *
     * 这个pool所需要的最小 segments 数量
     *
     * The minimum number of required segments for this pool.
     *
     * */
    private final int numberOfRequiredMemorySegments;

    /**
     * 当前可用的内存段。
     *
     * 这些是已从网络缓冲池请求的段，当前未作为缓冲实例分发。
     *
     * 当心:
     *
     *
     * The currently available memory segments. These are segments, which have been requested from
     * the network buffer pool and are currently not handed out as Buffer instances.
     *
     * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
     * locks acquired before entering this class vs. locks being acquired during calls to external
     * code inside this class, e.g. with {@link org.apache.flink.runtime.io.network.partition.consumer.BufferManager#bufferQueue} via the
     * {@link #registeredListeners} callback.
     */
    private final ArrayDeque<MemorySegment> availableMemorySegments =  new ArrayDeque<MemorySegment>();

    /**
     *
     * 缓冲区可用性侦听器，当缓冲区可用时需要通知侦听器。
     *
     * 侦听器只能在没有可用缓冲区实例的时间/状态下注册。
     *
     * 只有当buffer使用者发现bufferPool中无buffer可用的时候，才能去注册listener，
     * 用于在有buffer可用的时候收到通知。监听器必须实现BufferListener接口。
     *
     *
     * Buffer availability listeners, which need to be notified when a Buffer becomes available.
     * Listeners can only be registered at a time/state where no Buffer instance was available.
     */
    private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

    /**
     * 从  network buffers 分配的最大MemorySegments
     * Maximum number of network buffers to allocate.
     * */
    private final int maxNumberOfMemorySegments;

    /**
     * 当前pool的大小
     * The current size of this pool. */
    @GuardedBy("availableMemorySegments")
    private int currentPoolSize;

    /**
     *
     * 从网络缓冲池请求并通过该池以某种方式引用的所有内存段的数目（例如，包装在缓冲实例中或作为可用段）。
     *
     *
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    @GuardedBy("availableMemorySegments")
    private int numberOfRequestedMemorySegments;

    // 最大 Buffers  Channel ????
    private final int maxBuffersPerChannel;

    // 子 分区 buffer 数量
    @GuardedBy("availableMemorySegments")
    private final int[] subpartitionBuffersCount;

    //
    private final BufferRecycler[] subpartitionBufferRecyclers;

    @GuardedBy("availableMemorySegments")
    private int unavailableSubpartitionsCount = 0;

    @GuardedBy("availableMemorySegments")
    private boolean isDestroyed;

    @GuardedBy("availableMemorySegments")
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    @GuardedBy("availableMemorySegments")
    private boolean requestingWhenAvailable;

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
     * network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     */
    LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                Integer.MAX_VALUE,
                0,
                Integer.MAX_VALUE);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
     * number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments,
                0,
                Integer.MAX_VALUE);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
     * with a minimal and maximal number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     * @param numberOfSubpartitions number of subpartitions
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments,
            int numberOfSubpartitions,
            int maxBuffersPerChannel) {
        checkArgument(
                numberOfRequiredMemorySegments > 0,
                "Required number of memory segments (%s) should be larger than 0.",
                numberOfRequiredMemorySegments);

        checkArgument(
                maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
                "Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
                maxNumberOfMemorySegments,
                numberOfRequiredMemorySegments);

        LOG.debug(
                "Using a local buffer pool with {}-{} buffers",
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments);

        this.networkBufferPool = networkBufferPool;
        this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
        this.currentPoolSize = numberOfRequiredMemorySegments;
        this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;

        if (numberOfSubpartitions > 0) {
            checkArgument(
                    maxBuffersPerChannel > 0,
                    "Maximum number of buffers for each channel (%s) should be larger than 0.",
                    maxBuffersPerChannel);
        }

        this.subpartitionBuffersCount = new int[numberOfSubpartitions];
        subpartitionBufferRecyclers = new BufferRecycler[numberOfSubpartitions];
        for (int i = 0; i < subpartitionBufferRecyclers.length; i++) {
            subpartitionBufferRecyclers[i] = new SubpartitionBufferRecycler(i, this);
        }
        this.maxBuffersPerChannel = maxBuffersPerChannel;

        // Lock is only taken, because #checkAvailability asserts it. It's a small penalty for
        // thread safety.
        synchronized (this.availableMemorySegments) {
            if (checkAvailability()) {
                availabilityHelper.resetAvailable();
            }

            checkConsistentAvailability();
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public boolean isDestroyed() {
        synchronized (availableMemorySegments) {
            return isDestroyed;
        }
    }

    @Override
    public int getNumberOfRequiredMemorySegments() {
        return numberOfRequiredMemorySegments;
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return maxNumberOfMemorySegments;
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    @Override
    public int getNumBuffers() {
        synchronized (availableMemorySegments) {
            return currentPoolSize;
        }
    }

    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
    }

    // 获取一个buffer
    @Override
    public Buffer requestBuffer() {
        return toBuffer(requestMemorySegment());
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        return toBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
    }

    // 获取一个bufferBuilder。BufferBuilder是buffer的一个封装形式

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        // 其中toBufferBuilder把MemorySegment封装为BufferBuilder类型，
        // 设置内存回收器为LocalBufferPool自身。
        return toBufferBuilder(requestMemorySegmentBlocking(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    // 获取一个bufferBuilder。BufferBuilder是buffer的一个封装形式
    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
    }

    // toBuffer方法将MemorySegment封装为NetworkBuffer形式
    private Buffer toBuffer(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        // 设定buffer的回收器(recycle)为localBufferPool自己
        return new NetworkBuffer(memorySegment, this);
    }

    private BufferBuilder toBufferBuilder(MemorySegment memorySegment, int targetChannel) {
        if (memorySegment == null) {
            return null;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            return new BufferBuilder(memorySegment, this);
        } else {
            return new BufferBuilder(memorySegment, subpartitionBufferRecyclers[targetChannel]);
        }
    }

    // 通过之前分析的requestMemorySegment获取一段内存，如果没有获取到，
    // 则调用availabilityHelper的get方法阻塞，直到获取内存成功。
    // 有内存被回收的时候，get方法会返回。
    private MemorySegment requestMemorySegmentBlocking(int targetChannel)
            throws InterruptedException {
        MemorySegment segment;
        while ((segment = requestMemorySegment(targetChannel)) == null) {
            try {
                // wait until available
                getAvailableFuture().get();
            } catch (ExecutionException e) {
                LOG.error("The available future is completed exceptionally.", e);
                ExceptionUtils.rethrow(e);
            }
        }
        return segment;
    }

    @Nullable
    private MemorySegment requestMemorySegment(int targetChannel) {
        MemorySegment segment;
        synchronized (availableMemorySegments) {
            if (isDestroyed) {
                throw new IllegalStateException("Buffer pool is destroyed.");
            }

            // target channel over quota; do not return a segment
            if (targetChannel != UNKNOWN_CHANNEL
                    && subpartitionBuffersCount[targetChannel] >= maxBuffersPerChannel) {
                return null;
            }

            segment = availableMemorySegments.poll();

            if (segment == null) {
                return null;
            }

            if (targetChannel != UNKNOWN_CHANNEL) {
                if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
                    unavailableSubpartitionsCount++;
                }
            }

            if (!checkAvailability()) {
                availabilityHelper.resetUnavailable();
            }

            checkConsistentAvailability();
        }
        return segment;
    }

    @Nullable
    private MemorySegment requestMemorySegment() {
        return requestMemorySegment(UNKNOWN_CHANNEL);
    }

    private boolean requestMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        // 只有已请求内存数量小于localBufferPool大小的时候才去请求NetworkBufferPool的内存
        if (isRequestedSizeReached()) {
            return false;
        }

        checkState(
                !isDestroyed,
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");

        // 从 全局networkBufferPool可用内存队列中取出一个内存片段
        MemorySegment segment = networkBufferPool.requestMemorySegment();
        if (segment != null) {
            // 如果请求到内存，增加已请求内存数量计数器，将MemorySegment加入缓存
            availableMemorySegments.add(segment);
            numberOfRequestedMemorySegments++;
            return true;
        }
        return false;
    }

    /**
     * 从全局请求MemorySegment（如果可用）
     * 一旦有一个池可用，就尝试从全局池中获取缓冲区。
     *
     * Tries to obtain a buffer from global pool as soon as one pool is available.
     *
     * Note that
     * multiple {@link LocalBufferPool}s might wait on the future of the global pool, hence this
     * method double-check if a new buffer is really needed at the time it becomes available.
     */
    private void requestMemorySegmentFromGlobalWhenAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        if (requestingWhenAvailable) {
            return;
        }
        requestingWhenAvailable = true;

        assertNoException(
                networkBufferPool.getAvailableFuture().thenRun(this::onGlobalPoolAvailable));
    }

    private void onGlobalPoolAvailable() {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            requestingWhenAvailable = false;
            if (isDestroyed || availabilityHelper.isApproximatelyAvailable()) {
                // there is currently no benefit to obtain buffer from global; give other pools
                // precedent
                return;
            }

            // Check availability and potentially request the memory segment. The call may also
            // result in invoking
            // #requestMemorySegmentFromGlobalWhenAvailable again if no segment could be fetched
            // because of
            // concurrent requests from different LocalBufferPools.
            if (checkAvailability()) {
                // 通知获取内存...
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    private boolean checkAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        // 如果可用内存片段队列为空，直接从NetworkBufferPool请求内存
        if (!availableMemorySegments.isEmpty()) {
            return unavailableSubpartitionsCount == 0;
        }
        if (!isRequestedSizeReached()) {

            // requestMemorySegmentFromGlobal : 如果可用内存片段队列为空，直接从NetworkBufferPool请求内存
            if (requestMemorySegmentFromGlobal()) {
                return unavailableSubpartitionsCount == 0;
            } else {

                requestMemorySegmentFromGlobalWhenAvailable();
            }
        }
        return false;
    }

    private void checkConsistentAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        final boolean shouldBeAvailable =
                availableMemorySegments.size() > 0 && unavailableSubpartitionsCount == 0;
        checkState(
                availabilityHelper.isApproximatelyAvailable() == shouldBeAvailable,
                "Inconsistent availability: expected " + shouldBeAvailable);
    }

    @Override
    public void recycle(MemorySegment segment) {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    /**
     * recycle方法调用registeredListeners.poll()获取listener。
     * Listener的作用是当availableMemorySegments耗尽的时候，在bufferPool注册listener。
     * 当回收了内存，availableMemorySegments不再为空的时候，
     * bufferPool调用监听器的notifyBufferAvailable方法，告知现在有buffer可用。
     *
     * 只有在availableMemorySegments（可用内存段队列）为空并且没有被destroy的时候才能添加listener。
     *
     * @param segment
     * @param channel
     */
    private void recycle(MemorySegment segment, int channel) {
        BufferListener listener;
        CompletableFuture<?> toNotify = null;

        // 创建一个默认的NotificationResult，为缓存未使用
        NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;

        // 一直循环，确保如果listener返回内存不再使用的时候，再次执行这段逻辑将其回收
        while (!notificationResult.isBufferUsed()) {
            synchronized (availableMemorySegments) {

                if (channel != UNKNOWN_CHANNEL) {
                    if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
                        unavailableSubpartitionsCount--;
                    }
                }

                // 如果已请求的内存片段数量多于当前pool的大小，需要将内存归还
                // currentPoolSize可能会在运行的过程中调整(NetworkBufferPool的redistributeBuffers方法)

                if (isDestroyed || hasExcessBuffers()) {

                    // 返还内存给networkBufferPool处理
                    // numberOfRequestedMemorySegments 减1
                    // 调用networkBufferPool的recycle方法
                    returnMemorySegment(segment);
                    return;
                } else {
                    // 取出一个注册的listener
                    listener = registeredListeners.poll();

                    // 如果没有listener
                    if (listener == null) {

                        // 将该内存片段放入可用内存片段列表
                        availableMemorySegments.add(segment);
                        // only need to check unavailableSubpartitionsCount here because
                        // availableMemorySegments is not empty
                        if (!availabilityHelper.isApproximatelyAvailable()
                                && unavailableSubpartitionsCount == 0) {
                            // 如果回收内存后，availableMemorySegments从空变为非空，设置toNotify为AVAILABLE
                            toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        }
                        break;
                    }
                }

                checkConsistentAvailability();
            }
            // 如果获取到了listener，调用listener的notifyBufferAvailable方法，告诉listener buffer可用
            notificationResult = fireBufferAvailableNotification(listener, segment);
        }

        mayNotifyAvailable(toNotify);
    }

    private NotificationResult fireBufferAvailableNotification(
            BufferListener listener, MemorySegment segment) {
        // We do not know which locks have been acquired before the recycle() or are needed in the
        // notification and which other threads also access them.
        // -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock
        // (FLINK-9676)
        NotificationResult notificationResult =
                listener.notifyBufferAvailable(new NetworkBuffer(segment, this));

        // 如果listener需要更多的buffer
        if (notificationResult.needsMoreBuffers()) {
            synchronized (availableMemorySegments) {
                if (isDestroyed) {
                    // cleanup tasks how they would have been done if we only had one synchronized
                    // block
                    listener.notifyBufferDestroyed();
                } else {
                    // 再次把它注册为listener
                    registeredListeners.add(listener);
                }
            }
        }
        return notificationResult;
    }

    /** Destroy is called after the produce or consume phase of a task finishes. */
    @Override
    public void lazyDestroy() {
        // NOTE: if you change this logic, be sure to update recycle() as well!
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (!isDestroyed) {
                MemorySegment segment;
                while ((segment = availableMemorySegments.poll()) != null) {
                    returnMemorySegment(segment);
                }

                BufferListener listener;
                while ((listener = registeredListeners.poll()) != null) {
                    listener.notifyBufferDestroyed();
                }

                if (!isAvailable()) {
                    toNotify = availabilityHelper.getAvailableFuture();
                }

                isDestroyed = true;
            }
        }

        mayNotifyAvailable(toNotify);

        networkBufferPool.destroyBufferPool(this);
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        synchronized (availableMemorySegments) {
            if (!availableMemorySegments.isEmpty() || isDestroyed) {
                return false;
            }

            registeredListeners.add(listener);
            return true;
        }
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            // 检查重新设定的pool size必须要大于或等于numberOfRequiredMemorySegments
            checkArgument(
                    numBuffers >= numberOfRequiredMemorySegments,
                    "Buffer pool needs at least %s buffers, but tried to set to %s",
                    numberOfRequiredMemorySegments,
                    numBuffers);

            // 设置currentPoolSize
            currentPoolSize = Math.min(numBuffers, maxNumberOfMemorySegments);

            // 如果已请求的内存片段数量超过了localBufferPool的大小
            // 将多出来的内存片段取出
            // 归还给NetworkBufferPool并回收
            returnExcessMemorySegments();

            if (isDestroyed) {
                // FLINK-19964: when two local buffer pools are released concurrently, one of them
                // gets buffers assigned
                // make sure that checkAvailability is not called as it would pro-actively acquire
                // one buffer from NetworkBufferPool
                return;
            }

            if (checkAvailability()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            } else {
                availabilityHelper.resetUnavailable();
            }

            checkConsistentAvailability();
        }

        mayNotifyAvailable(toNotify);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public String toString() {
        synchronized (availableMemorySegments) {
            return String.format(
                    "[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d,"
                            + "subpartitions: %d, maxBuffersPerChannel: %d, destroyed: %s]",
                    currentPoolSize,
                    numberOfRequiredMemorySegments,
                    numberOfRequestedMemorySegments,
                    availableMemorySegments.size(),
                    maxNumberOfMemorySegments,
                    registeredListeners.size(),
                    subpartitionBuffersCount.length,
                    maxBuffersPerChannel,
                    isDestroyed);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Notifies the potential segment consumer of the new available segments by completing the
     * previous uncompleted future.
     */
    private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    // 归还内存的逻辑
    private void returnMemorySegment(MemorySegment segment) {
        assert Thread.holdsLock(availableMemorySegments);

        // 已请求内存数量的技术器减1
        numberOfRequestedMemorySegments--;
        // 调用NetworkBufferPool的recycle方法，回收这一段内存
        networkBufferPool.recycle(segment);
    }

    private void returnExcessMemorySegments() {
        assert Thread.holdsLock(availableMemorySegments);

        // 如果已请求的内存数量超过了bufferPool的大小，一直循环
        while (hasExcessBuffers()) {
            // 从可用内存队列中取出一个内存片段
            MemorySegment segment = availableMemorySegments.poll();
            if (segment == null) {
                return;
            }

            // 归还这个内存
            returnMemorySegment(segment);
        }
    }

    private boolean hasExcessBuffers() {
        return numberOfRequestedMemorySegments > currentPoolSize;
    }

    private boolean isRequestedSizeReached() {
        return numberOfRequestedMemorySegments >= currentPoolSize;
    }

    @VisibleForTesting
    @Override
    public BufferRecycler[] getSubpartitionBufferRecyclers() {
        return subpartitionBufferRecyclers;
    }

    private static class SubpartitionBufferRecycler implements BufferRecycler {

        private int channel;
        private LocalBufferPool bufferPool;

        SubpartitionBufferRecycler(int channel, LocalBufferPool bufferPool) {
            this.channel = channel;
            this.bufferPool = bufferPool;
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            bufferPool.recycle(memorySegment, channel);
        }
    }
}
