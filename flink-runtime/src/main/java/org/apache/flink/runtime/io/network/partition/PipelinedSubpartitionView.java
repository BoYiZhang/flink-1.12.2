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

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** View over a pipelined in-memory only subpartition. */
public class PipelinedSubpartitionView implements ResultSubpartitionView {

    /**
     * 标识这个视图归属于哪个  PipelinedSubpartition
     * The subpartition this view belongs to. */
    private final PipelinedSubpartition parent;

    /**
     * 当有数据的时候通过BufferAvailabilityListener的实现通知
     * LocalInputChannel
     * 或者
     * CreditBasedSequenceNumberingViewReader(RemoteInputChannel)有数据到来,可以消费数据
     */
    private final BufferAvailabilityListener availabilityListener;

    /**
     * 这个视图是否被释放
     * Flag indicating whether this view has been released. */
    final AtomicBoolean isReleased;

    public PipelinedSubpartitionView(
            PipelinedSubpartition parent, BufferAvailabilityListener listener) {
        this.parent = checkNotNull(parent);
        this.availabilityListener = checkNotNull(listener);
        // 原子类, 资源是否释放.
        this.isReleased = new AtomicBoolean();
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        return parent.pollBuffer();
    }

    @Override
    public void notifyDataAvailable() {
        //回调接口,通知inputchannel有数据到来
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public void notifyPriorityEvent(int priorityBufferNumber) {
        //回调接口,通知inputchannel有事件到来
        availabilityListener.notifyPriorityEvent(priorityBufferNumber);
    }

    // 释放所有的资源
    @Override
    public void releaseAllResources() {
        if (isReleased.compareAndSet(false, true)) {
            // The view doesn't hold any resources and the parent cannot be restarted. Therefore,
            // it's OK to notify about consumption as well.
            parent.onConsumedSubpartition();
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased.get() || parent.isReleased();
    }

    @Override
    public void resumeConsumption() {
        parent.resumeConsumption();
    }

    @Override
    public boolean isAvailable(int numCreditsAvailable) {
        return parent.isAvailable(numCreditsAvailable);
    }

    @Override
    public Throwable getFailureCause() {
        return parent.getFailureCause();
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return parent.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public String toString() {
        return String.format(
                "%s(index: %d) of ResultPartition %s",
                this.getClass().getSimpleName(),
                parent.getSubPartitionIndex(),
                parent.parent.getPartitionId());
    }
}
