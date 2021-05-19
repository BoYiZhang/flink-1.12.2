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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The general buffer manager used by {@link InputChannel} to request/recycle exclusive or floating
 * buffers.
 */
public class BufferManager implements BufferListener, BufferRecycler {

    /**
     *
     * AvailableBufferQueue负责维护RemoteInputChannel的可用buffer
     *
     *
     * The available buffer queue wraps both exclusive and requested floating buffers.
     *
     * */
    private final AvailableBufferQueue bufferQueue = new AvailableBufferQueue();

    /** The buffer provider for requesting exclusive buffers. */
    private final MemorySegmentProvider globalPool;

    /** The input channel to own this buffer manager. */
    private final InputChannel inputChannel;

    /**
     * The tag indicates whether it is waiting for additional floating buffers from the buffer pool.
     */
    @GuardedBy("bufferQueue")
    private boolean isWaitingForFloatingBuffers;

    /** The total number of required buffers for the respective input channel. */
    @GuardedBy("bufferQueue")
    private int numRequiredBuffers;

    public BufferManager(
            MemorySegmentProvider globalPool, InputChannel inputChannel, int numRequiredBuffers) {

        this.globalPool = checkNotNull(globalPool);
        this.inputChannel = checkNotNull(inputChannel);
        checkArgument(numRequiredBuffers >= 0);
        this.numRequiredBuffers = numRequiredBuffers;
    }

    // ------------------------------------------------------------------------
    // Buffer request
    // ------------------------------------------------------------------------

    @Nullable
    Buffer requestBuffer() {
        // 其中bufferQueue对象为AvailableBufferQueue类型。
        //AvailableBufferQueue负责维护RemoteInputChannel的可用内存 .
        synchronized (bufferQueue) {
            return bufferQueue.takeBuffer();
        }
    }

    Buffer requestBufferBlocking() throws InterruptedException {
        synchronized (bufferQueue) {
            Buffer buffer;
            while ((buffer = bufferQueue.takeBuffer()) == null) {
                if (inputChannel.isReleased()) {
                    throw new CancelTaskException(
                            "Input channel ["
                                    + inputChannel.channelInfo
                                    + "] has already been released.");
                }
                if (!isWaitingForFloatingBuffers) {
                    BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
                    buffer = bufferPool.requestBuffer();
                    if (buffer == null && shouldContinueRequest(bufferPool)) {
                        continue;
                    }
                }

                if (buffer != null) {
                    return buffer;
                }
                // 阻塞请求...
                bufferQueue.wait();
            }
            return buffer;
        }
    }

    private boolean shouldContinueRequest(BufferPool bufferPool) {
        if (bufferPool.addBufferListener(this)) {
            isWaitingForFloatingBuffers = true;
            numRequiredBuffers = 1;
            return false;
        } else if (bufferPool.isDestroyed()) {
            throw new CancelTaskException("Local buffer pool has already been released.");
        } else {
            return true;
        }
    }

    /** Requests exclusive buffers from the provider. */
    void requestExclusiveBuffers(int numExclusiveBuffers) throws IOException {
        // numExclusiveBuffers : MemorySegment为批量申请，该变量决定一批次申请的MemorySegment的数量
        Collection<MemorySegment> segments = globalPool.requestMemorySegments(numExclusiveBuffers);
        checkArgument(
                !segments.isEmpty(),
                "The number of exclusive buffers per channel should be larger than 0.");

        synchronized (bufferQueue) {
            for (MemorySegment segment : segments) {
                // 此处使用NetworkBuffer封装了memorySegment。
                //因此该内存块回收的时候会调用RemoteInputChannel的recycle方法
                bufferQueue.addExclusiveBuffer(
                        new NetworkBuffer(segment, this), numRequiredBuffers);
            }
        }
    }

    /**
     * Requests floating buffers from the buffer pool based on the given required amount, and
     * returns the actual requested amount. If the required amount is not fully satisfied, it will
     * register as a listener.
     */
    int requestFloatingBuffers(int numRequired) {
        int numRequestedBuffers = 0;

        // 锁定bufferQueue
        synchronized (bufferQueue) {
            // Similar to notifyBufferAvailable(), make sure that we never add a buffer after
            // channel
            // released all buffers via releaseAllResources().

            // 避免在releaseAllResources()之后执行
            if (inputChannel.isReleased()) {
                return numRequestedBuffers;
            }

            numRequiredBuffers = numRequired;

            // while循环不断的从inputGate的bufferPool（即LocalBufferPool）获取可用的缓存块，
            //加入到浮动缓存队列中，同时记录请求的缓存块数量，
            //直到可用缓存数量不再小于需要缓存块的数量时候停止循环。

            while (bufferQueue.getAvailableBufferSize() < numRequiredBuffers
                    && !isWaitingForFloatingBuffers) {
                // 获取 BufferPool
                BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
                // 请求内存
                Buffer buffer = bufferPool.requestBuffer();
                if (buffer != null) {
                    // 添加到浮动内存..
                    bufferQueue.addFloatingBuffer(buffer);
                    numRequestedBuffers++;
                } else if (bufferPool.addBufferListener(this)) {
                    // 如果请求不到buffer（channel没有足够的buffer）
                    // 注册一个监听器，并且标记等待请求浮动Buffers的状态为true
                    isWaitingForFloatingBuffers = true;
                    break;
                }
            }
        }
        return numRequestedBuffers;
    }

    // ------------------------------------------------------------------------
    // Buffer recycle
    // ------------------------------------------------------------------------

    /**
     * Exclusive buffer is recycled to this channel manager directly and it may trigger return extra
     * floating buffer based on <tt>numRequiredBuffers</tt>.
     *
     * @param segment The exclusive segment of this channel.
     */
    @Override
    public void recycle(MemorySegment segment) {
        int numAddedBuffers = 0;
        synchronized (bufferQueue) {
            try {
                // Similar to notifyBufferAvailable(), make sure that we never add a buffer
                // after channel released all buffers via releaseAllResources().
                if (inputChannel.isReleased()) {
                    // 使用globalPool#recycleMemorySegments    回收内存
                    globalPool.recycleMemorySegments(Collections.singletonList(segment));
                } else {

                    // 添加内存片段到专属内存
                    numAddedBuffers =
                            bufferQueue.addExclusiveBuffer(
                                    new NetworkBuffer(segment, this), numRequiredBuffers);
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            } finally {
                bufferQueue.notifyAll();
            }
        }

        try {
            inputChannel.notifyBufferAvailable(numAddedBuffers);
        } catch (Throwable t) {
            ExceptionUtils.rethrow(t);
        }
    }

    void releaseFloatingBuffers() {
        synchronized (bufferQueue) {
            numRequiredBuffers = 0;
            bufferQueue.releaseFloatingBuffers();
        }
    }

    /**
     * 释放所有的Buffers
     *
     * Recycles all the exclusive and floating buffers from the given buffer queue.
     * */
    void releaseAllBuffers(ArrayDeque<Buffer> buffers) throws IOException {
        // Gather all exclusive buffers and recycle them to global pool in batch, because
        // we do not want to trigger redistribution of buffers after each recycle.
        final List<MemorySegment> exclusiveRecyclingSegments = new ArrayList<>();

        Buffer buffer;
        while ((buffer = buffers.poll()) != null) {
            if (buffer.getRecycler() == this) {
                exclusiveRecyclingSegments.add(buffer.getMemorySegment());
            } else {
                buffer.recycleBuffer();
            }
        }
        synchronized (bufferQueue) {
           //   释放RemoteInputChannel所有缓存
            bufferQueue.releaseAll(exclusiveRecyclingSegments);
            bufferQueue.notifyAll();
        }

        if (exclusiveRecyclingSegments.size() > 0) {
            globalPool.recycleMemorySegments(exclusiveRecyclingSegments);
        }
    }

    // ------------------------------------------------------------------------
    // Buffer listener notification
    // ------------------------------------------------------------------------

    /**
     *
     * LocalBufferPool 通知有 buffer 可用
     *
     * The buffer pool notifies this listener of an available floating buffer. If the listener is
     * released or currently does not need extra buffers, the buffer should be returned to the
     * buffer pool. Otherwise, the buffer will be added into the <tt>bufferQueue</tt>.
     *
     * @param buffer Buffer that becomes available in buffer pool.
     * @return NotificationResult indicates whether this channel accepts the buffer and is waiting
     *     for more floating buffers.
     */
    @Override
    public BufferListener.NotificationResult notifyBufferAvailable(Buffer buffer) {
        BufferListener.NotificationResult notificationResult =
                BufferListener.NotificationResult.BUFFER_NOT_USED;

        // Assuming two remote channels with respective buffer managers as listeners inside
        // LocalBufferPool.
        // While canceler thread calling ch1#releaseAllResources, it might trigger
        // bm2#notifyBufferAvaialble.
        // Concurrently if task thread is recycling exclusive buffer, it might trigger
        // bm1#notifyBufferAvailable.
        // Then these two threads will both occupy the respective bufferQueue lock and wait for
        // other side's
        // bufferQueue lock to cause deadlock. So we check the isReleased state out of synchronized
        // to resolve it.
        if (inputChannel.isReleased()) {
            return notificationResult;
        }

        try {
            synchronized (bufferQueue) {
                checkState(
                        isWaitingForFloatingBuffers,
                        "This channel should be waiting for floating buffers.");

                // Important: make sure that we never add a buffer after releaseAllResources()
                // released all buffers. Following scenarios exist:
                // 1) releaseAllBuffers() already released buffers inside bufferQueue
                // -> while isReleased is set correctly in InputChannel
                // 2) releaseAllBuffers() did not yet release buffers from bufferQueue
                // -> we may or may not have set isReleased yet but will always wait for the
                // lock on bufferQueue to release buffers
                if (inputChannel.isReleased()
                        || bufferQueue.getAvailableBufferSize() >= numRequiredBuffers) {
                    isWaitingForFloatingBuffers = false;
                    return notificationResult;
                }

                //增加floating buffer
                bufferQueue.addFloatingBuffer(buffer);
                bufferQueue.notifyAll();

                if (bufferQueue.getAvailableBufferSize() == numRequiredBuffers) {
                    //bufferQueue中有足够多的 buffer 了
                    isWaitingForFloatingBuffers = false;
                    notificationResult = BufferListener.NotificationResult.BUFFER_USED_NO_NEED_MORE;
                } else {
                    //bufferQueue 中 buffer 仍然不足
                    notificationResult = BufferListener.NotificationResult.BUFFER_USED_NEED_MORE;
                }
            }

            if (notificationResult != NotificationResult.BUFFER_NOT_USED) {
                inputChannel.notifyBufferAvailable(1);
            }
        } catch (Throwable t) {
            inputChannel.setError(t);
        }

        return notificationResult;
    }

    @Override
    public void notifyBufferDestroyed() {
        // Nothing to do actually.
    }

    // ------------------------------------------------------------------------
    // Getter properties
    // ------------------------------------------------------------------------

    @VisibleForTesting
    int unsynchronizedGetNumberOfRequiredBuffers() {
        return numRequiredBuffers;
    }

    @VisibleForTesting
    boolean unsynchronizedIsWaitingForFloatingBuffers() {
        return isWaitingForFloatingBuffers;
    }

    @VisibleForTesting
    int getNumberOfAvailableBuffers() {
        synchronized (bufferQueue) {
            return bufferQueue.getAvailableBufferSize();
        }
    }

    int unsynchronizedGetAvailableExclusiveBuffers() {
        return bufferQueue.exclusiveBuffers.size();
    }

    int unsynchronizedGetFloatingBuffersAvailable() {
        return bufferQueue.floatingBuffers.size();
    }


    /**
     * AvailableBufferQueue负责维护RemoteInputChannel的可用buffer。
     *
     * 它的内部维护了两个内存队列，分别为floatingBuffers(浮动buffer)和exclusiveBuffers(专用buffer)。
     *
     *
     *
     *

     * exclusiveBuffers 是在创建InputChannel的时候分配的（setup方法，间接调用了NetworkBufferPool的requestMemorySegments）
     * exclusiveBuffers的大小是固定的，归RemoteInputChannel独享。
     * 如果出现专属buffer不够用的情况，会申请浮动buffer。
     *
     * 浮动buffer在InputChannel所属的bufferPool中申请
     * 所有属于同一个inputGate的inputChannel共享这一个bufferPool。
     * 总结来说浮动buffer可以理解为是按需分配，从公有的bufferPool中"借用"的。
     *
     *
     * Manages the exclusive and floating buffers of this channel, and handles the internal buffer
     * related logic.
     */
    static final class AvailableBufferQueue {

        /**
         *
         * 固定缓冲池中当前可用的浮动缓冲区。
         *
         * 浮动buffer在InputChannel所属的bufferPool中申请
         * 所有属于同一个inputGate的inputChannel共享这一个bufferPool。
         * 总结来说浮动buffer可以理解为是按需分配，从公有的bufferPool中"借用"的。
         *
         * The current available floating buffers from the fixed buffer pool.
         * */
        final ArrayDeque<Buffer> floatingBuffers;

        /**
         * 全局缓冲池中当前可用的独占缓冲区。
         *
         * exclusiveBuffers 是在创建InputChannel的时候分配的（setup方法方法，间接调用了NetworkBufferPool的requestMemorySegments）
         * exclusiveBuffers的大小是固定的，归RemoteInputChannel独享。
         * 如果出现专属buffer不够用的情况，会申请浮动buffer。
         *
         * The current available exclusive buffers from the global buffer pool.
         * */
        final ArrayDeque<Buffer> exclusiveBuffers;

        AvailableBufferQueue() {
            this.exclusiveBuffers = new ArrayDeque<>();
            this.floatingBuffers = new ArrayDeque<>();
        }

        /**
         * 为队列增加专属buffer的方法为addExclusiveBuffer
         *
         * 在InputGate的setup阶段为所有的input channel分配专属内存。查看SingleInputGate的setup方法
         *
         * Adds an exclusive buffer (back) into the queue and recycles one floating buffer if the
         * number of available buffers in queue is more than the required amount.
         *
         * @param buffer The exclusive buffer to add
         * @param numRequiredBuffers The number of required buffers
         * @return How many buffers were added to the queue
         */
        int addExclusiveBuffer(Buffer buffer, int numRequiredBuffers) {

            // 1. 添加buffer到专属buffer队列。
            exclusiveBuffers.add(buffer);

            // 2. 如果可用buffer数（浮动buffer+专属buffer）大于所需内存buffer数，回收一个浮动buffer。
            if (getAvailableBufferSize() > numRequiredBuffers) {
                // 超过所需要的 Buffer , 释放...
                Buffer floatingBuffer = floatingBuffers.poll();
                if (floatingBuffer != null) {
                    // 释放浮动
                    floatingBuffer.recycleBuffer();
                    // 如果回收了浮动内存，返回0，否则返回1。
                    return 0;
                }
            }
            // 如果回收了浮动内存，返回0，否则返回1。
            return 1;
        }

        void addFloatingBuffer(Buffer buffer) {
            floatingBuffers.add(buffer);
        }

        /**
         * Takes the floating buffer first in order to make full use of floating buffers reasonably.
         *
         * @return An available floating or exclusive buffer, may be null if the channel is
         *     released.
         */
        @Nullable
        Buffer takeBuffer() {
            // 请求内存的逻辑位于takeBuffer方法，如下面所示：
            // 如果有可用的浮动内存，会优先使用他们。没有可用浮动内存的时候会使用专属内存。
            if (floatingBuffers.size() > 0) {
                return floatingBuffers.poll();
            } else {
                return exclusiveBuffers.poll();
            }
        }

        /**
         * The floating buffer is recycled to local buffer pool directly, and the exclusive buffer
         * will be gathered to return to global buffer pool later.
         *
         * @param exclusiveSegments The list that we will add exclusive segments into.
         */
        void releaseAll(List<MemorySegment> exclusiveSegments) {
            Buffer buffer;
            while ((buffer = floatingBuffers.poll()) != null) {
                buffer.recycleBuffer();
            }
            while ((buffer = exclusiveBuffers.poll()) != null) {
                exclusiveSegments.add(buffer.getMemorySegment());
            }
        }

        void releaseFloatingBuffers() {
            Buffer buffer;
            while ((buffer = floatingBuffers.poll()) != null) {
                buffer.recycleBuffer();
            }
        }

        int getAvailableBufferSize() {
            return floatingBuffers.size() + exclusiveBuffers.size();
        }
    }
}
