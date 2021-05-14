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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * NetworkBufferPool是供网络层使用的，固定大小的缓存池。
 *
 * 每个task并非直接从NetworkBufferPool获取内存，而是使用从NetworkBufferPool创建出的LocalBufferPool来分配内存。
 *

 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances for the network
 * stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 */
public class NetworkBufferPool
        implements BufferPoolFactory, MemorySegmentProvider, AvailabilityProvider {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

    // 默认应该是 4096个.
    private final int totalNumberOfMemorySegments;

    // 默认 32k  = 32768 byte
    private final int memorySegmentSize;

    // 一个队列，用来存放可用的MemorySegment
    private final ArrayDeque<MemorySegment> availableMemorySegments;

    private volatile boolean isDestroyed;

    // ---- Managed buffer pools ----------------------------------------------

    private final Object factoryLock = new Object();

    //  用来存放基于此NetworkBufferPool创建的LocalBufferPool
    private final Set<LocalBufferPool> allBufferPools = new HashSet<>();

    // 总共需要的buffer数量。统计所有已经分配给LocalBufferPool的buffer数量
    private int numTotalRequiredBuffers;

    // 请求内存的最大等待时间（超时时间）
    private final Duration requestSegmentsTimeout;

    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    @VisibleForTesting
    public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
        this(numberOfSegmentsToAllocate, segmentSize, Duration.ofMillis(Integer.MAX_VALUE));
    }

    /** Allocates all {@link MemorySegment} instances managed by this pool. */
    public NetworkBufferPool(
            int numberOfSegmentsToAllocate, int segmentSize, Duration requestSegmentsTimeout) {

        // 设置总MemorySegment数量 :  4096 = (128 * 1024 * 1024) /(32 *1024)
        this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;

        // 设置每个MemorySegment的大小 : 32 *1024
        this.memorySegmentSize = segmentSize;

        Preconditions.checkNotNull(requestSegmentsTimeout);
        checkArgument(
                requestSegmentsTimeout.toMillis() > 0,
                "The timeout for requesting exclusive buffers should be positive.");

        // 设置请求内存的超时时间
        this.requestSegmentsTimeout = requestSegmentsTimeout;

        // MemorySegment大小转换为long
        final long sizeInLong = (long) segmentSize;

        try {
            // 创建availableMemorySegments队列 :
            //              这个队列只能从头或者为移除数据, 做不到移除指定元素.
            this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);
        } catch (OutOfMemoryError err) {
            throw new OutOfMemoryError(
                    "Could not allocate buffer queue of length "
                            + numberOfSegmentsToAllocate
                            + " - "
                            + err.getMessage());
        }

        try {
            // 循环调用，创建numberOfSegmentsToAllocate个MemorySegment
            // 这里分配的是堆外内存 : 4096个 ?
            for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
                availableMemorySegments.add(
                        MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
            }
        } catch (OutOfMemoryError err) {
            int allocated = availableMemorySegments.size();

            // free some memory
            availableMemorySegments.clear();

            long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;
            long allocatedMb = (sizeInLong * allocated) >> 20;
            long missingMb = requiredMb - allocatedMb;

            throw new OutOfMemoryError(
                    "Could not allocate enough memory segments for NetworkBufferPool "
                            + "(required (Mb): "
                            + requiredMb
                            + ", allocated (Mb): "
                            + allocatedMb
                            + ", missing (Mb): "
                            + missingMb
                            + "). Cause: "
                            + err.getMessage());
        }

        // 设置状态为可用
        availabilityHelper.resetAvailable();

        // 分配的内次你空间大小 :  32k*4096 / 1024 = 128Mb
        long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;

        LOG.info(
                "Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
                allocatedMb,
                availableMemorySegments.size(),
                segmentSize);
    }

    @Nullable
    public MemorySegment requestMemorySegment() {
        synchronized (availableMemorySegments) {
            // 调用internalRequestMemorySegment
            return internalRequestMemorySegment();
        }
    }

    // 回收单个MemorySegment的方法为recycle。
    public void recycle(MemorySegment segment) {
        // Adds the segment back to the queue, which does not immediately free the memory
        // however, since this happens when references to the global pool are also released,
        // making the availableMemorySegments queue and its contained object reclaimable
        internalRecycleMemorySegments(Collections.singleton(checkNotNull(segment)));
    }

    // 批量请求 内存的方法
    // numberOfSegmentsToRequest : MemorySegment为批量申请，该变量决定一批次申请的MemorySegment的数量
    @Override
    public List<MemorySegment> requestMemorySegments(int numberOfSegmentsToRequest)
            throws IOException {
        checkArgument(
                numberOfSegmentsToRequest > 0,
                "Number of buffers to request must be larger than 0.");

        synchronized (factoryLock) {
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            // 尝试重新分配内存
            tryRedistributeBuffers(numberOfSegmentsToRequest);
        }

        // 创建容纳请求到的内存的容器
        final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);
        try {
            // 统计请求内存操作耗时
            final Deadline deadline = Deadline.fromNow(requestSegmentsTimeout);
            while (true) {
                if (isDestroyed) {
                    throw new IllegalStateException("Buffer pool is destroyed.");
                }

                MemorySegment segment;
                synchronized (availableMemorySegments) {
                    // 调用之前分析过的internalRequestMemorySegment方法
                    // 从availableMemorySegments中获取一个内存片段
                    // 如果没有获取成功，等得2000毫秒
                    if ((segment = internalRequestMemorySegment()) == null) {
                        availableMemorySegments.wait(2000);
                    }
                }
                // 如果获取成功，加入内存到segments容器
                if (segment != null) {
                    segments.add(segment);
                }

                // 如果请求到的内存数量超过了单批次请求的数量限制，退出循环
                if (segments.size() >= numberOfSegmentsToRequest) {
                    break;
                }

                // 如果申请内存操作超时，抛出异常
                if (!deadline.hasTimeLeft()) {
                    throw new IOException(
                            String.format(
                                    "Timeout triggered when requesting exclusive buffers: %s, "
                                            + " or you may increase the timeout which is %dms by setting the key '%s'.",
                                    getConfigDescription(),
                                    requestSegmentsTimeout.toMillis(),
                                    NettyShuffleEnvironmentOptions
                                            .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS
                                            .key()));
                }
            }
        } catch (Throwable e) {
            recycleMemorySegments(segments, numberOfSegmentsToRequest);
            ExceptionUtils.rethrowIOException(e);
        }

        return segments;
    }

    @Nullable
    private MemorySegment internalRequestMemorySegment() {
        assert Thread.holdsLock(availableMemorySegments);

        // 从availableMemorySegments获取一个内存片段
        final MemorySegment segment = availableMemorySegments.poll();
        // 如果获取这个内存成功（不为null）之后availableMemorySegments变为empty，设置状态为不可用
        if (availableMemorySegments.isEmpty() && segment != null) {
            availabilityHelper.resetUnavailable();
        }
        return segment;
    }

    @Override
    public void recycleMemorySegments(Collection<MemorySegment> segments) {
        recycleMemorySegments(segments, segments.size());
    }

    private void recycleMemorySegments(Collection<MemorySegment> segments, int size) {

        internalRecycleMemorySegments(segments);

        synchronized (factoryLock) {
            numTotalRequiredBuffers -= size;

            // note: if this fails, we're fine for the buffer pool since we already recycled the
            // segments

            // 内存回收成功之后，需要重新分配内存
            redistributeBuffers();
        }
    }

    private void internalRecycleMemorySegments(Collection<MemorySegment> segments) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {

            // 如果可以成功归还内存，需要设置availabilityHelper为可用状态
            if (availableMemorySegments.isEmpty() && !segments.isEmpty()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
            // 加入归还的内存片段到availableMemorySegments
            availableMemorySegments.addAll(segments);
            // 通知所有等待调用的对象
            availableMemorySegments.notifyAll();
        }

        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    public void destroy() {
        synchronized (factoryLock) {
            isDestroyed = true;
        }

        synchronized (availableMemorySegments) {
            MemorySegment segment;
            while ((segment = availableMemorySegments.poll()) != null) {
                segment.free();
            }
        }
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }

    public int getTotalNumberOfMemorySegments() {
        return isDestroyed() ? 0 : totalNumberOfMemorySegments;
    }

    public long getTotalMemory() {
        return getTotalNumberOfMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    public long getAvailableMemory() {
        return getNumberOfAvailableMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfUsedMemorySegments() {
        return getTotalNumberOfMemorySegments() - getNumberOfAvailableMemorySegments();
    }

    public long getUsedMemory() {
        return getNumberOfUsedMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfRegisteredBufferPools() {
        synchronized (factoryLock) {
            return allBufferPools.size();
        }
    }

    public int countBuffers() {
        int buffers = 0;

        synchronized (factoryLock) {
            for (BufferPool bp : allBufferPools) {
                buffers += bp.getNumBuffers();
            }
        }

        return buffers;
    }

    /** Returns a future that is completed when there are free segments in this pool. */
    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    // ------------------------------------------------------------------------
    // BufferPoolFactory
    // ------------------------------------------------------------------------

    @Override
    public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers)
            throws IOException {
        return internalCreateBufferPool(numRequiredBuffers, maxUsedBuffers, 0, Integer.MAX_VALUE);
    }

    @Override
    public BufferPool createBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel)
            throws IOException {
        return internalCreateBufferPool(
                numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    }


    // numRequiredBuffers：必须要有的buffer数量
    // maxUsedBuffers：使用的buffer数量不能超过这个值 :  默认值 8 .  控制参数: taskmanager.network.memory.floating-buffers-per-gate
    // bufferPoolOwner：bufferPool所有者，用于回收内存时通知owner
    // numSubpartitions 子分区的数量
    private BufferPool internalCreateBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel)
            throws IOException {

        // It is necessary to use a separate lock from the one used for buffer
        // requests to ensure deadlock freedom for failure cases.

        //  synchronized!!!!!!!!!!!!!!!
        synchronized (factoryLock) {
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            // Ensure that the number of required buffers can be satisfied.
            // With dynamic memory management this should become obsolete.

            // 检查内存分配不能超过NetworkBufferPool总容量
            if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
                throw new IOException(
                        String.format(
                                "Insufficient number of network buffers: "
                                        + "required %d, but only %d available. %s.",
                                numRequiredBuffers,
                                totalNumberOfMemorySegments - numTotalRequiredBuffers,
                                getConfigDescription()));
            }

            // 更新所有LocalBufferPool占用的总内存数量
            this.numTotalRequiredBuffers += numRequiredBuffers;

            // 创建一个LocalBufferPool
            // 我们可以开始了，创建一个新的缓冲池并重新分配非固定大小的缓冲区。
            // We are good to go, create a new buffer pool and redistribute non-fixed size buffers.
            LocalBufferPool localBufferPool =
                    new LocalBufferPool(
                            this,
                            numRequiredBuffers,
                            maxUsedBuffers,
                            numSubpartitions,
                            maxBuffersPerChannel);

            // 记录新创建的bufferPool到allBufferPools集合
            allBufferPools.add(localBufferPool);

            // 重新分配每个LocalBufferPool的内存块，接下来分析
            redistributeBuffers();

            return localBufferPool;
        }
    }

    @Override
    public void destroyBufferPool(BufferPool bufferPool) {
        if (!(bufferPool instanceof LocalBufferPool)) {
            throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
        }

        synchronized (factoryLock) {
            if (allBufferPools.remove(bufferPool)) {
                numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();

                redistributeBuffers();
            }
        }
    }

    /**
     * Destroys all buffer pools that allocate their buffers from this buffer pool (created via
     * {@link #createBufferPool(int, int)}).
     */
    public void destroyAllBufferPools() {
        synchronized (factoryLock) {
            // create a copy to avoid concurrent modification exceptions
            LocalBufferPool[] poolsCopy =
                    allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

            for (LocalBufferPool pool : poolsCopy) {
                pool.lazyDestroy();
            }

            // some sanity checks
            if (allBufferPools.size() > 0 || numTotalRequiredBuffers > 0) {
                throw new IllegalStateException(
                        "NetworkBufferPool is not empty after destroying all LocalBufferPools");
            }
        }
    }

    // numberOfSegmentsToRequest : MemorySegment为批量申请，该变量决定一批次申请的MemorySegment的数量
    //Must be called from synchronized block
    private void tryRedistributeBuffers(int numberOfSegmentsToRequest) throws IOException {
        assert Thread.holdsLock(factoryLock);

        if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
            throw new IOException(
                    String.format(
                            "Insufficient number of network buffers: "
                                    + "required %d, but only %d available. %s.",
                            numberOfSegmentsToRequest,
                            totalNumberOfMemorySegments - numTotalRequiredBuffers,
                            getConfigDescription()));
        }

        this.numTotalRequiredBuffers += numberOfSegmentsToRequest;

        try {
            redistributeBuffers();
        } catch (Throwable t) {
            this.numTotalRequiredBuffers -= numberOfSegmentsToRequest;

            redistributeBuffers();
            ExceptionUtils.rethrow(t);
        }
    }

    // Must be called from synchronized block
    private void redistributeBuffers() {
        assert Thread.holdsLock(factoryLock);

        // All buffers, which are not among the required ones
        // 计算出尚未分配的内存块数量
        final int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

        // 如果没有可用的内存
        // 即所有的内存都分配给了各个LocalBufferPool
        // 需要设置每个bufferPool的大小为每个pool要求的最小内存大小
        // 通知LocalBufferPool 归还超出数量的内存
        if (numAvailableMemorySegment == 0) {
            // in this case, we need to redistribute buffers so that every pool gets its minimum
            for (LocalBufferPool bufferPool : allBufferPools) {
                bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
            }
            return;
        }

        /*
         * With buffer pools being potentially limited, let's distribute the available memory
         * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
         * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
         * it may be less. Based on this and the sum of all these values (totalCapacity), we build
         * a ratio that we use to distribute the buffers.
         */

        long totalCapacity = 0; // long to avoid int overflow

        // 统计所有的bufferPool可以被额外分配的内存数量总和
        for (LocalBufferPool bufferPool : allBufferPools) {

            // 计算每个bufferPool可以最多超量使用的内存数
            int excessMax =
                    bufferPool.getMaxNumberOfMemorySegments()
                            - bufferPool.getNumberOfRequiredMemorySegments();

            // 如果最多可超量使用的内存数量比numAvailableMemorySegment还多
            // 肯定无法满足这个需求
            // 按照numAvailableMemorySegment来统计
            totalCapacity += Math.min(numAvailableMemorySegment, excessMax);
        }

        // 如果总容量为0，直接返回
        // no capacity to receive additional buffers?
        if (totalCapacity == 0) {
            return; // necessary to avoid div by zero when nothing to re-distribute
        }

        // since one of the arguments of 'min(a,b)' is a positive int, this is actually
        // guaranteed to be within the 'int' domain
        // (we use a checked downCast to handle possible bugs more gracefully).

        // 计算可以重分配的内存数量
        // MathUtils.checkedDownCast用来确保结果转换成int不会溢出
        final int memorySegmentsToDistribute =
                MathUtils.checkedDownCast(Math.min(numAvailableMemorySegment, totalCapacity));

        long totalPartsUsed = 0; // of totalCapacity
        int numDistributedMemorySegment = 0;

        // 对每个LocalBufferPool重新设置pool size
        for (LocalBufferPool bufferPool : allBufferPools) {

            // 计算pool的最大可超量使用的内存数
            int excessMax =
                    bufferPool.getMaxNumberOfMemorySegments()
                            - bufferPool.getNumberOfRequiredMemorySegments();

            // shortcut
            // 如果可超量分配的内存数量为0，不用进行后续操作
            if (excessMax == 0) {
                continue;
            }

            // 和totalCapacity统计相同
            totalPartsUsed += Math.min(numAvailableMemorySegment, excessMax);

            // avoid remaining buffers by looking at the total capacity that should have been
            // re-distributed up until here
            // the downcast will always succeed, because both arguments of the subtraction are in
            // the 'int' domain

            // 计算每个pool可重分配（增加）的内存数量
            // totalPartsUsed / totalCapacity可以计算出已分配的内存占据总可用内存的比例
            // 因为增加totalPartsUsed在实际分配内存之前，所以这个比例包含了即将分配内存的这个LocalBufferPool的占比
            // memorySegmentsToDistribute * totalPartsUsed / totalCapacity可计算出已分配的内存数量（包含即将分配buffer的这个LocalBufferPool）
            // 这个结果再减去numDistributedMemorySegment（已分配内存数量），最终得到需要分配给此bufferPool的内存数量
            final int mySize =
                    MathUtils.checkedDownCast(
                            memorySegmentsToDistribute * totalPartsUsed / totalCapacity
                                    - numDistributedMemorySegment);

            // 更新已分配内存数量的统计
            numDistributedMemorySegment += mySize;

            // 重新设置bufferPool的大小为新的值
            bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize);
        }

        assert (totalPartsUsed == totalCapacity);
        assert (numDistributedMemorySegment == memorySegmentsToDistribute);
    }

    private String getConfigDescription() {
        return String.format(
                "The total number of network buffers is currently set to %d of %d bytes each. "
                        + "You can increase this number by setting the configuration keys '%s', '%s', and '%s'",
                totalNumberOfMemorySegments,
                memorySegmentSize,
                TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                TaskManagerOptions.NETWORK_MEMORY_MAX.key());
    }
}
