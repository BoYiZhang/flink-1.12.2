/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Disposable;

import java.io.Serializable;

/**
 *
 * 流运算符的基本接口。
 * Basic interface for stream operators.
 *
 * Implementers would implement one of {@link
 * org.apache.flink.streaming.api.operators.OneInputStreamOperator} or {@link
 * org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators that process
 * elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} offers
 * default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with methods
 * on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface StreamOperator<OUT>
        extends CheckpointListener, KeyContext, Disposable, Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * 在处理任何元素之前立即调用此方法，它应该包含运算符的初始化逻辑。
     *
     * 在恢复的情况下，此方法需要确保在传回控制之前处理所有恢复的数据，
     * 以便在恢复操作链期间确保元素的顺序（从尾部操作器到头部操作器打开操作器）。
     *
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic.
     *
     * @implSpec In case of recovery, this method needs to ensure that all recovered data is
     *     processed before passing back control, so that the order of elements is ensured during
     *     the recovery of an operator chain (operators are opened from the tail operator to the
     *     head operator).
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void open() throws Exception;

    /**
     *
     * 通过方法将所有记录添加到运算符后，将调用此方法
     *
     * 该方法将刷新所有剩余的缓冲数据。
     * 应传播缓冲区刷新期间的异常，以便将操作识别为失败，因为最后的数据项未正确处理。
     *
     * This method is called after all records have been added to the operators via the methods
     * {@link
     * org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)},
     * or {@link
     * org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)}
     * and {@link
     * org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.
     *
     * <p>The method is expected to flush all remaining buffered data.
     * Exceptions during this flushing of buffered should be propagated, in order to cause the operation to be recognized
     * as failed, because the last data items are not processed properly.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void close() throws Exception;

    /**
     * 无论是在操作成功完成的情况下，还是在失败和取消的情况下，都会在operator's生命周期的最后调用此方法。
     *
     * This method is called at the very end of the operator's life, both in the case of a
     * successful completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources that the
     * operator has acquired.
     */
    @Override
    void dispose() throws Exception;

    // ------------------------------------------------------------------------
    //  state snapshots
    // ------------------------------------------------------------------------

    /**
     * 当操作员应该在发出自己的检查点屏障之前执行快照时，会调用此方法。
     *
     * 此方法不适用于任何实际的状态持久化，而仅用于在触发检查点屏障之前发出一些数据。
     *
     * 维护一些对检查点无效的小瞬态的操作符，但是可以在触发检查点之前简单地发送到下游。
     * (尤其是当需要以可重新扩展的方式对其进行检查时)
     *
     * 一个例子是`opportunistic`的预聚合操作符，它具有较小的预聚合状态，该状态经常被刷新到下游。
     *
     * 此方法不应用于任何实际状态快照逻辑，
     * 因为它本质上将位于操作员检查站的同步部分内。
     * 如果在此方法中完成繁重的工作，则将影响等待时间和下游检查点对齐。
     *
     *
     * This method is called when the operator should do a snapshot, before it emits its own
     * checkpoint barrier.
     *
     * <p>This method is intended not for any actual state persistence, but only for emitting some
     * data before emitting the checkpoint barrier.
     *
     * Operators that maintain some small transient state that is inefficient to checkpoint (especially when it would need to be checkpointed in
     * a re-scalable way) but can simply be sent downstream before the checkpoint.
     *
     * An example are opportunistic pre-aggregation operators, which have small the pre-aggregation state that is
     * frequently flushed downstream.
     *
     * <p><b>Important:</b> This method should not be used for any actual state snapshot logic,
     * because it will inherently be within the synchronous part of the operator's checkpoint. If
     * heavy work is done within this method, it will affect latency and downstream checkpoint
     * alignments.
     *
     * @param checkpointId The ID of the checkpoint.
     * @throws Exception Throwing an exception here causes the operator to fail and go into
     *     recovery.
     */
    void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * 请求绘制 operator  的状态快照。
     *
     * Called to draw a state snapshot from the operator.
     *
     * @return a runnable future to the state handle that points to the snapshotted state. For
     *     synchronous implementations, the runnable might already be finished.
     * @throws Exception exception that happened during snapshotting.
     */
    OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception;

    /** Provides a context to initialize all state in the operator. */
    void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception;

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    void setKeyContextElement1(StreamRecord<?> record) throws Exception;

    void setKeyContextElement2(StreamRecord<?> record) throws Exception;

    MetricGroup getMetricGroup();

    OperatorID getOperatorID();
}
