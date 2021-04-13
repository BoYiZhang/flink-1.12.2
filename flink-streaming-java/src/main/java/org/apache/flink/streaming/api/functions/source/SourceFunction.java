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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 *
 * Base interface for all stream data sources in Flink
 *
 * 一. stream source 的约定如下:
 *
 * 1. 当source开始发射元素时，{@link#run}方法被调用，其中{@link SourceContext}可用于发射元素。
 * 2. run方法可以根据需要运行任意长的时间。
 * 3. 可以调用{@link #cancel()} 方法退出run 方法
 *
 * 二.  CheckpointedFunction Sources
 *      Sources可以实现 {@link  org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} 接口必须保证  state checkpointing , 内部状态更新 和 元素发送不会同时进行.
 *      可以在synchronized代码块汇总 使用提供的 checkpointing lock object 锁 来保护 状态更新和 发送数据.
 *
 *      这是实现  checkpointed source 时应遵循的基本模式：

 * 三. Timestamps and watermarks
 *
 *  Sources可以为数据分配timestamps，并且可以手动发出watermarks 。
 *      但是，只有当 streaming program 在{@link TimeCharacteristic#EventTime}上运行时，才会有效。
 *  在({@link TimeCharacteristic#IngestionTime} and {@link  TimeCharacteristic#ProcessingTime})模式watermarks就会失效.
 *
 *
 * The contract of a stream source is the following:
 *
 * When the source should start emitting elements, the {@link #run} method is called with a {@link SourceContext} that can be used for emitting elements.
 * The run method can run for as long as necessary.
 * The source must, however, react to an invocation of {@link #cancel()} by breaking out of its main loop.
 *
 * <h3>CheckpointedFunction Sources</h3>
 *
 * <p>Sources that also implement the {@link
 * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} interface must ensure that state
 * checkpointing, updating of internal state and emission of elements are not done concurrently.
 *
 * This is achieved by using the provided checkpointing lock object to protect update of state and
 * emission of elements in a synchronized block.
 *
 * <p>This is the basic pattern one should follow when implementing a checkpointed source:
 *
 * <pre>{@code
 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      private transient ListState<Long> checkpointedCount;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              // this synchronized block ensures that state checkpointing,
 *              // internal state updates and emission of elements are an atomic operation
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public void initializeState(FunctionInitializationContext context) {
 *          this.checkpointedCount = context
 *              .getOperatorStateStore()
 *              .getListState(new ListStateDescriptor<>("count", Long.class));
 *
 *          if (context.isRestored()) {
 *              for (Long count : this.checkpointedCount.get()) {
 *                  this.count = count;
 *              }
 *          }
 *      }
 *
 *      public void snapshotState(FunctionSnapshotContext context) {
 *          this.checkpointedCount.clear();
 *          this.checkpointedCount.add(count);
 *      }
 * }
 * }</pre>
 *
 * <h3>Timestamps and watermarks:</h3>
 *
 *  Sources可以为数据分配timestamps，并且可以手动发出watermarks 。
 *      但是，只有当 streaming program 在{@link TimeCharacteristic#EventTime}上运行时，才会有效。
 *  在({@link TimeCharacteristic#IngestionTime} and {@link  TimeCharacteristic#ProcessingTime})模式watermarks就会失效.
 *
 * <p>Sources may assign timestamps to elements and may manually emit watermarks.
 * However, these are only interpreted if the streaming program runs on {@link TimeCharacteristic#EventTime}.
 *
 * On other time characteristics ({@link TimeCharacteristic#IngestionTime} and {@link
 * TimeCharacteristic#ProcessingTime}), the watermarks from the source function are ignored.
 *
 * @param <T> The type of the elements produced by this source.
 * @see org.apache.flink.streaming.api.TimeCharacteristic
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

    /**
     * Starts the source. Implementations can use the {@link SourceContext} emit elements.
     *
     * <p>Sources that implement {@link
     * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} must lock on the checkpoint
     * lock (using a synchronized block) before updating internal state and emitting elements, to
     * make both an atomic operation:
     *
     * <pre>{@code
     *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
     *      private long count = 0L;
     *      private volatile boolean isRunning = true;
     *
     *      private transient ListState<Long> checkpointedCount;
     *
     *      public void run(SourceContext<T> ctx) {
     *          while (isRunning && count < 1000) {
     *              // this synchronized block ensures that state checkpointing,
     *              // internal state updates and emission of elements are an atomic operation
     *              synchronized (ctx.getCheckpointLock()) {
     *                  ctx.collect(count);
     *                  count++;
     *              }
     *          }
     *      }
     *
     *      public void cancel() {
     *          isRunning = false;
     *      }
     *
     *      public void initializeState(FunctionInitializationContext context) {
     *          this.checkpointedCount = context
     *              .getOperatorStateStore()
     *              .getListState(new ListStateDescriptor<>("count", Long.class));
     *
     *          if (context.isRestored()) {
     *              for (Long count : this.checkpointedCount.get()) {
     *                  this.count = count;
     *              }
     *          }
     *      }
     *
     *      public void snapshotState(FunctionSnapshotContext context) {
     *          this.checkpointedCount.clear();
     *          this.checkpointedCount.add(count);
     *      }
     * }
     * }</pre>
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    void run(SourceContext<T> ctx) throws Exception;

    /**
     * Cancels the source. Most sources will have a while loop inside the {@link
     * #run(SourceContext)} method. The implementation needs to ensure that the source will break
     * out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>When a source is canceled, the executing thread will also be interrupted (via {@link
     * Thread#interrupt()}). The interruption happens strictly after this method has been called, so
     * any interruption handler can rely on the fact that this method has completed. It is good
     * practice to make any flags altered by this method "volatile", in order to guarantee the
     * visibility of the effects of this method to any interruption handler.
     */
    void cancel();

    // ------------------------------------------------------------------------
    //  source context
    // ------------------------------------------------------------------------

    /**
     * Interface that source functions use to emit elements, and possibly watermarks.
     *
     * @param <T> The type of the elements produced by the source.
     */
    @Public // Interface might be extended in the future with additional methods.
    interface SourceContext<T> {

        /**
         * 从source获取一条没有附加timestamp的数据, 在大多数情况下,这是默认的数据获取方式
         *
         * element的时间戳取决于streaming program的 time characteristic
         *
         * Emits one element from the source, without attaching a timestamp. In most cases, this is
         * the default way of emitting elements.
         *
         * <p>The timestamp that the element will get assigned depends on the time characteristic of
         * the streaming program:
         *
         * <ul>
         *   <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.
         *   <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's current
         *       time as the timestamp.
         *   <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp
         *       initially. It needs to get a timestamp (via a {@link TimestampAssigner}) before any
         *       time-dependent operation (like time windows).
         * </ul>
         *
         * @param element The element to emit
         */
        void collect(T element);

        /**
         *
         * 发送一条具有时间戳的数据.
         *
         * 此方法与使用{@link TimeCharacteristic＃EventTime}的程序有关，
         * sources在其中自己分配时间戳，而不是依赖流上的{@link TimestampAssigner}
         *
         * 在某些特定时间，此timestamp可能会被忽略或覆盖。
         * 这允许程序在不同的时间特征和行为之间切换，而无需更改源函数的代码。
         *
         * Emits one element from the source, and attaches the given timestamp. This method is
         * relevant for programs using {@link TimeCharacteristic#EventTime}, where the sources
         * assign timestamps themselves, rather than relying on a {@link TimestampAssigner} on the
         * stream.
         *
         * <p>On certain time characteristics, this timestamp may be ignored or overwritten. This
         * allows programs to switch between the different time characteristics and behaviors
         * without changing the code of the source functions.
         *
         * <ul>
         *   <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
         *       because processing time never works with element timestamps.
         *   <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
         *       system's current time, to realize proper ingestion time semantics.
         *   <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.
         * </ul>
         *
         * @param element The element to emit
         * @param timestamp The timestamp in milliseconds since the Epoch
         */
        @PublicEvolving
        void collectWithTimestamp(T element, long timestamp);

        /**
         * 发送指定的Watermark .
         *
         * 值{@code t}的watermarks声明不会再出现带有时间戳{@code t'<= t}的元素。
         * Watermark 的 value 为 `t`, 代表了后续数据的时间戳t' ,  不会出现t'<= t的情况.
         * 如果收到数据的时间戳t' < t ,则认为这些数据是 late
         *
         * 仅当在{@link TimeCharacteristic#EventTime}上运行时，此方法才有意义
         * 在{@link TimeCharacteristic#ProcessingTime}，watermarks将被忽略。
         * 在{@link TimeCharacteristic＃IngestionTime}上，watermarks将被自动ingestion时间 代替。
         *
         *
         * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
         * elements with a timestamp {@code t' <= t} will occur any more. If further such elements
         * will be emitted, those elements are considered <i>late</i>.
         *
         * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}. On
         * {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On {@link
         * TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the automatic
         * ingestion time watermarks.
         *
         * @param mark The Watermark to emit
         */
        @PublicEvolving
        void emitWatermark(Watermark mark);

        /**
         * 标记当前source 为暂时空闲.
         * 这将告诉系统，该源将在一段不确定的时间内暂时停止发射记录和水印。
         *
         *
         * 这仅在{@link TimeCharacteristic#IngestionTime}和{@link TimeCharacteristic#EventTime}上运行时才相关，
         * 允许下游任务提前处理其水印，而无需在该源空闲时等待来自该源的水印。
         *
         * Source functions 应该尽可能的在它们知道自己处于空闲状态时调用这个方法。
         *
         * 当重新触发以下三个放的时候. 系统会考虑再次恢复执行.
         *      {@link SourceContext#collect(T)},
         *      {@link SourceContext#collectWithTimestamp(T, long)}, or
         *      {@link SourceContext#emitWatermark(Watermark)}
         *
         * 一旦调用{@link SourceContext#collect（T）}、{@link SourceContext#collectWithTimestamp（T，long）}或{@link SourceContext#emitWatermark（Watermark）}从源中发出元素或水印，系统将考虑再次恢复活动。
         *
         * Marks the source to be temporarily idle.
         *
         * This tells the system that this source will temporarily stop emitting records and watermarks for an indefinite amount of time.
         *
         * This is only relevant when running on {@link TimeCharacteristic#IngestionTime} and {@link
         * TimeCharacteristic#EventTime}, allowing downstream tasks to advance their watermarks
         * without the need to wait for watermarks from this source while it is idle.
         *
         * <p>Source functions should make a best effort to call this method as soon as they
         * acknowledge themselves to be idle.
         *
         * The system will consider the source to resume activity again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T,
         * long)}, or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or watermarks from the source.
         */
        @PublicEvolving
        void markAsTemporarilyIdle();

        /**
         * 返回一个checkpoint lock
         *
         * 有关如何编写一致的检查点源的详细信息，请参阅{@link SourceFunction}中的类 注释。
         *
         *
         * Returns the checkpoint lock.
         *
         * Please refer to the class-level comment in {@link SourceFunction} for details about how to write a consistent checkpointed source.
         *
         * @return The object to use as the lock
         */
        Object getCheckpointLock();


        /**
         * 请求系统关闭环境
         * This method is called by the system to shut down the context. */
        void close();
    }
}
