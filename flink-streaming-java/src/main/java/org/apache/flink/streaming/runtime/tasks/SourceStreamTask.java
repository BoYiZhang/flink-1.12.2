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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in a
 * synchronized block that locks on the lock Object. Also, the modification of the state and the
 * emission of elements must happen in the same block of code that is protected by the synchronized
 * block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class SourceStreamTask<
                OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends StreamTask<OUT, OP> {

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;

    /**
     * Indicates whether this Task was purposefully finished (by finishTask()), in this case we want
     * to ignore exceptions thrown after finishing, to ensure shutdown works smoothly.
     */
    private volatile boolean wasStoppedExternally = false;

    public SourceStreamTask(Environment env) throws Exception {
        this(env, new Object());
    }

    private SourceStreamTask(Environment env, Object lock) throws Exception {
        super(
                env,
                null,
                FatalExitExceptionHandler.INSTANCE,
                StreamTaskActionExecutor.synchronizedExecutor(lock));
        this.lock = Preconditions.checkNotNull(lock);
        this.sourceThread = new LegacySourceFunctionThread();
    }

    @Override
    protected void init() {
        // we check if the source is actually inducing the checkpoints, rather
        // than the trigger
        SourceFunction<?> source = mainOperator.getUserFunction();
        if (source instanceof ExternallyInducedSource) {
            externallyInducedCheckpoints = true;

            ExternallyInducedSource.CheckpointTrigger triggerHook =
                    new ExternallyInducedSource.CheckpointTrigger() {

                        @Override
                        public void triggerCheckpoint(long checkpointId) throws FlinkException {
                            // TODO - we need to see how to derive those. We should probably not
                            // encode this in the
                            // TODO -   source's trigger message, but do a handshake in this task
                            // between the trigger
                            // TODO -   message from the master, and the source's trigger
                            // notification
                            final CheckpointOptions checkpointOptions =
                                    CheckpointOptions.forCheckpointWithDefaultLocation(
                                            configuration.isExactlyOnceCheckpointMode(),
                                            configuration.isUnalignedCheckpointsEnabled(),
                                            configuration.getAlignmentTimeout());
                            final long timestamp = System.currentTimeMillis();

                            final CheckpointMetaData checkpointMetaData =
                                    new CheckpointMetaData(checkpointId, timestamp);

                            try {
                                SourceStreamTask.super
                                        .triggerCheckpointAsync(
                                                checkpointMetaData, checkpointOptions)
                                        .get();
                            } catch (RuntimeException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new FlinkException(e.getMessage(), e);
                            }
                        }
                    };

            ((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
        }
        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos);
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        mainOperator.advanceToEndOfEventTime();
    }

    @Override
    protected void cleanup() {
        // does not hold any resources, so no cleanup needed
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

        controller.suspendDefaultAction();

        // Against the usual contract of this method, this implementation is not step-wise but
        // blocking instead for
        // compatibility reasons with the current source interface (source functions run as a loop,
        // not in steps).
        sourceThread.setTaskDescription(getName());
        sourceThread.start();
        sourceThread
                .getCompletionFuture()
                .whenComplete(
                        (Void ignore, Throwable sourceThreadThrowable) -> {
                            if (isCanceled()
                                    && ExceptionUtils.findThrowable(
                                                    sourceThreadThrowable,
                                                    InterruptedException.class)
                                            .isPresent()) {
                                mailboxProcessor.reportThrowable(
                                        new CancelTaskException(sourceThreadThrowable));
                            } else if (!wasStoppedExternally && sourceThreadThrowable != null) {
                                mailboxProcessor.reportThrowable(sourceThreadThrowable);
                            } else {
                                mailboxProcessor.allActionsCompleted();
                            }
                        });
    }

    @Override
    protected void cleanUpInvoke() throws Exception {
        if (isFailing()) {
            interruptSourceThread(true);
        }
        super.cleanUpInvoke();
    }

    @Override
    protected void cancelTask() {
        cancelTask(true);
    }

    @Override
    protected void finishTask() {
        wasStoppedExternally = true;
        /**
         * Currently stop with savepoint relies on the EndOfPartitionEvents propagation and performs
         * clean shutdown after the stop with savepoint (which can produce some records to process
         * after the savepoint while stopping). If we interrupt source thread, we might leave the
         * network stack in an inconsistent state. So, if we want to relay on the clean shutdown, we
         * can not interrupt the source thread.
         */
        cancelTask(false);
    }

    private void cancelTask(boolean interrupt) {
        try {
            if (mainOperator != null) {
                mainOperator.cancel();
            }
        } finally {
            interruptSourceThread(interrupt);
        }
    }

    private void interruptSourceThread(boolean interrupt) {
        if (sourceThread.isAlive()) {
            if (interrupt) {
                sourceThread.interrupt();
            }
        } else if (!sourceThread.getCompletionFuture().isDone()) {
            // source thread didn't start
            sourceThread.getCompletionFuture().complete(null);
        }
    }

    @Override
    protected CompletableFuture<Void> getCompletionFuture() {
        return sourceThread.getCompletionFuture();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public Future<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (!externallyInducedCheckpoints) {
            return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        if (!externallyInducedCheckpoints) {
            super.declineCheckpoint(checkpointId);
        }
    }

    /** Runnable that executes the the source function in the head operator. */
    private class LegacySourceFunctionThread extends Thread {

        private final CompletableFuture<Void> completionFuture;

        LegacySourceFunctionThread() {
            this.completionFuture = new CompletableFuture<>();
        }


        //    this = {SourceStreamTask$LegacySourceFunctionThread@7173} "Thread[Legacy Source Thread - Source: Socket Stream (1/1)#0,5,Flink Task Threads]"
        //        completionFuture = {CompletableFuture@7180} "java.util.concurrent.CompletableFuture@3093fc5f[Not completed]"
        //        this$0 = {SourceStreamTask@7181} "Source: Socket Stream (1/1)#0"
        //        name = "Legacy Source Thread - Source: Socket Stream (1/1)#0"
        //        priority = 5
        //        threadQ = null
        //        eetop = 140547506958336
        //        single_step = false
        //        daemon = false
        //        stillborn = false
        //        target = null
        //        group = {ThreadGroup@6367} "java.lang.ThreadGroup[name=Flink Task Threads,maxpri=10]"
        //        contextClassLoader = {FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader@6876}
        //        inheritedAccessControlContext = {AccessControlContext@7188}
        //        threadLocals = null
        //        inheritableThreadLocals = {ThreadLocal$ThreadLocalMap@7189}
        //        stackSize = 0
        //        nativeParkEventPointer = 0
        //        tid = 88
        //        threadStatus = 5
        //        parkBlocker = null
        //        blocker = null
        //        blockerLock = {Object@7190}
        //        uncaughtExceptionHandler = null
        //        threadLocalRandomSeed = 0
        //        threadLocalRandomProbe = 0
        //        threadLocalRandomSecondarySeed = 0
        //        completionFuture = {CompletableFuture@7180} "java.util.concurrent.CompletableFuture@3093fc5f[Not completed]"
        //        operatorChain = {OperatorChain@7182}
        //        lock = {Object@7183}
        //        mainOperator = {StreamSource@7184}

        @Override
        public void run() {
            try {



            // mainOperator = {StreamSource@7184}
            //    ctx = null
            //    canceledOrStopped = false
            //    hasSentMaxWatermark = false
            //    userFunction = {SocketTextStreamFunction@7209}
            //            hostname = "192.168.101.30"
            //            port = 9999
            //            delimiter = "\n"
            //            maxNumRetries = 0
            //            delayBetweenRetries = 500
            //            currentSocket = null
            //            isRunning = true
            //    functionsClosed = false
            //    chainingStrategy = {ChainingStrategy@7210} "HEAD"
            //    container = {SourceStreamTask@7181} "Source: Socket Stream (1/1)#0"
            //            sourceThread = {SourceStreamTask$LegacySourceFunctionThread@7173} "Thread[Legacy Source Thread - Source: Socket Stream (1/1)#0,5,Flink Task Threads]"
            //            lock = {Object@7183}
            //            externallyInducedCheckpoints = false
            //            isFinished = false
            //            actionExecutor = {StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor@7222}
            //            inputProcessor = null
            //            mainOperator = {StreamSource@7184}
            //            operatorChain = {OperatorChain@7182}
            //            configuration = {StreamConfig@7211} "\n=======================Stream Config=======================\nNumber of non-chained inputs: 0\nNumber of non-chained outputs: 1\nOutput names: [(Source: Socket Stream-1 -> Flat Map-2, typeNumber=0, outputPartitioner=REBALANCE, bufferTimeout=-1, outputTag=null)]\nPartitioning:\n\t2: REBALANCE\nChained subtasks: []\nOperator: SimpleUdfStreamOperatorFactory\nState Monitoring: false"
            //                    config = {Configuration@7239} "{checkpointing=false, serializedUDF=[B@3a6b972f, graphContainingLoops=false, vertexID=1, execution.checkpointing.unaligned=false, inputs=[B@7f690e06, typeSerializer_out=[B@50b764b0, sorted-inputs=false, chainEnd=true, nonChainedOutputs=[B@4edcf777, numberOfOutputs=1, operatorName=Source: Socket Stream, chainedTaskConfig_=[B@28e8c36c, chainIndex=1, execution.checkpointing.alignment-timeout=0, timechar=2, chainedOutputs=[B@1f46f149, edgesInOrder=[B@743f84f5, managedMemFraction.STATE_BACKEND=0.0, isChainedSubtask=true, checkpointMode=1, operatorID=[B@5df1b24a, statekeyser=[B@33ece93b}"
            //                            confData = {HashMap@7241}  size = 23
            //                                    "checkpointing" -> {Boolean@7268} false
            //                                    "serializedUDF" -> {byte[1244]@7270} [-84, -19, 0, 5, 115, 114, 0, 71, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 102, 108, 105, 110, 107, 46, 115, 116, 114, 101, 97, 109, 105, 110, 103, 46, 97, 112, 105, 46, 111, 112, 101, 114, 97, 116, 111, 114, 115, 46, 83, 105, 109, 112, 108, 101, 85, 100, 102, 83, 116, 114, 101, 97, 109, 79, 112, 101, 114, 97, 116, 111, 114, 70, 97, 99, 116, 111, 114, 121, 12, 86, 3, -11, 40, -51, 98, 34, 2, 0, 1, 76, 0, 8, 111, 112, 101, 114, 97, 116, 111, +1,144 more]
            //                                    "graphContainingLoops" -> {Boolean@7268} false
            //                                    "vertexID" -> {Integer@7273} 1
            //                                    "execution.checkpointing.unaligned" -> {Boolean@7268} false
            //                                    "inputs" -> {byte[91]@7276} [-84, -19, 0, 5, 117, 114, 0, 64, 91, 76, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 102, 108, 105, 110, 107, 46, 115, 116, 114, 101, 97, 109, 105, 110, 103, 46, 97, 112, 105, 46, 103, 114, 97, 112, 104, 46, 83, 116, 114, 101, 97, 109, 67, 111, 110, 102, 105, 103, 36, 73, 110, 112, 117, 116, 67, 111, 110, 102, 105, 103, 59, -60, -19, -113, 38, 88, 57, -77, -88, 2, 0, 0, 120, 112, 0, 0, 0, 2, 112, 112]
            //                                    "typeSerializer_out" -> {byte[228]@7278} [-84, -19, 0, 5, 115, 114, 0, 59, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 102, 108, 105, 110, 107, 46, 97, 112, 105, 46, 99, 111, 109, 109, 111, 110, 46, 116, 121, 112, 101, 117, 116, 105, 108, 115, 46, 98, 97, 115, 101, 46, 83, 116, 114, 105, 110, 103, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 120, 114, 0, 66, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 102, 108, 105, 110, 107, 46, 97, +128 more]
            //                                    "sorted-inputs" -> {Boolean@7268} false
            //                                    "chainEnd" -> {Boolean@7281} true
            //                                    "nonChainedOutputs" -> {byte[902]@7283} [-84, -19, 0, 5, 115, 114, 0, 19, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76, 105, 115, 116, 120, -127, -46, 29, -103, -57, 97, -99, 3, 0, 1, 73, 0, 4, 115, 105, 122, 101, 120, 112, 0, 0, 0, 1, 119, 4, 0, 0, 0, 1, 115, 114, 0, 47, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 102, 108, 105, 110, 107, 46, 115, 116, 114, 101, 97, 109, 105, 110, 103, 46, 97, 112, 105, 46, 103, 114, 97, 112, 104, 46, 83, 116, +802 more]
            //                                    "numberOfOutputs" -> {Integer@7273} 1
            //                                    "operatorName" -> "Source: Socket Stream"
            //                                    "chainedTaskConfig_" -> {byte[5]@7288} [-84, -19, 0, 5, 112]
            //                                    "chainIndex" -> {Integer@7273} 1
            //                                    "execution.checkpointing.alignment-timeout" -> {Long@7291} 0
            //                                    "timechar" -> {Integer@7293} 2
            //                                    "chainedOutputs" -> {byte[58]@7295} [-84, -19, 0, 5, 115, 114, 0, 19, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76, 105, 115, 116, 120, -127, -46, 29, -103, -57, 97, -99, 3, 0, 1, 73, 0, 4, 115, 105, 122, 101, 120, 112, 0, 0, 0, 0, 119, 4, 0, 0, 0, 0, 120]
            //                                    "edgesInOrder" -> {byte[902]@7297} [-84, -19, 0, 5, 115, 114, 0, 19, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76, 105, 115, 116, 120, -127, -46, 29, -103, -57, 97, -99, 3, 0, 1, 73, 0, 4, 115, 105, 122, 101, 120, 112, 0, 0, 0, 1, 119, 4, 0, 0, 0, 1, 115, 114, 0, 47, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 102, 108, 105, 110, 107, 46, 115, 116, 114, 101, 97, 109, 105, 110, 103, 46, 97, 112, 105, 46, 103, 114, 97, 112, 104, 46, 83, 116, +802 more]
            //                                    "managedMemFraction.STATE_BACKEND" -> {Double@7299} 0.0
            //                                    "isChainedSubtask" -> {Boolean@7281} true
            //                                    "checkpointMode" -> {Integer@7273} 1
            //                                    "operatorID" -> {byte[16]@7303} [-68, 118, 76, -40, -35, -9, -96, -49, -15, 38, -11, 28, 22, 35, -106, 88]
            //                                    "statekeyser" -> {byte[5]@7305} [-84, -19, 0, 5, 112]
            //
            //            stateBackend = {MemoryStateBackend@7223} "MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)"
            //            subtaskCheckpointCoordinator = {SubtaskCheckpointCoordinatorImpl@7224}
            //            timerService = {SystemProcessingTimeService@7225}
            //            cancelables = {CloseableRegistry@7226}
            //            asyncExceptionHandler = {StreamTask$StreamTaskAsyncExceptionHandler@7227}
            //            isRunning = true
            //            canceled = false
            //            failing = false
            //            disposedOperators = false
            //            asyncOperationsThreadPool = {ThreadPoolExecutor@7228} "java.util.concurrent.ThreadPoolExecutor@3e54043d[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]"
            //            recordWriter = {SingleRecordWriter@7229}
            //            mailboxProcessor = {MailboxProcessor@7230}
            //            mainMailboxExecutor = {MailboxExecutorImpl@7231}
            //            channelIOExecutor = {Executors$FinalizableDelegatedExecutorService@7232}
            //            syncSavepointId = null
            //            latestAsyncCheckpointStartDelayNanos = 0
            //            environment = {RuntimeEnvironment@7233}
            //            shouldInterruptOnCancel = true
            //
            //    config = {StreamConfig@7211} "\n=======================Stream Config=======================\nNumber of non-chained inputs: 0\nNumber of non-chained outputs: 1\nOutput names: [(Source: Socket Stream-1 -> Flat Map-2, typeNumber=0, outputPartitioner=REBALANCE, bufferTimeout=-1, outputTag=null)]\nPartitioning:\n\t2: REBALANCE\nChained subtasks: []\nOperator: SimpleUdfStreamOperatorFactory\nState Monitoring: false"
            //    output = {CountingOutput@7212}
            //            output = {RecordWriterOutput@7306}
            //                    channelSelector = {RebalancePartitioner@7311} "REBALANCE"
            //                    targetPartition = {PipelinedResultPartition@7312} "PipelinedResultPartition f3e53e3fbc3eab68b25ea79f80873233#0@9bd406565dca544a85576fd06acc0fc0 [PIPELINED_BOUNDED, 4 subpartitions, 4 pending consumptions]"
            //                    numberOfChannels = 4
            //                    serializer = {DataOutputSerializer@7313} "[pos=0 cap=128]"
            //                    rng = {XORShiftRandom@7314}
            //                    flushAlways = false
            //                    outputFlusher = {RecordWriter$OutputFlusher@6594} "Thread[OutputFlusher for Source: Socket Stream,5,Flink Task Threads]"
            //                    flusherException = null
            //                    volatileFlusherException = null
            //                    volatileFlusherExceptionCheckSkipCount = 0
            //            recordWriter = {ChannelSelectorRecordWriter@7308}
            //            serializationDelegate = {SerializationDelegate@7309}
            //            streamStatusProvider = {OperatorChain@7182}
            //            outputTag = null
            //            watermarkGauge = {WatermarkGauge@7310}
            //            numRecordsOut = {SimpleCounter@7307}
            //    runtimeContext = {StreamingRuntimeContext@7213}
            //    stateKeySelector1 = null
            //    stateKeySelector2 = null
            //    stateHandler = {StreamOperatorStateHandler@7214}
            //    timeServiceManager = null
            //    metrics = {OperatorMetricGroup@7215}
            //    latencyStats = {LatencyStats@7216}
            //    processingTimeService = {ProcessingTimeServiceImpl@7217}
            //    combinedWatermark = -9223372036854775808
            //    input1Watermark = -9223372036854775808
            //    input2Watermark = -9223372036854775808

                mainOperator.run(lock, getStreamStatusMaintainer(), operatorChain);
                if (!wasStoppedExternally && !isCanceled()) {
                    synchronized (lock) {
                        operatorChain.setIgnoreEndOfInput(false);
                    }
                }
                completionFuture.complete(null);
            } catch (Throwable t) {
                // Note, t can be also an InterruptedException
                completionFuture.completeExceptionally(t);
            }
        }

        public void setTaskDescription(final String taskDescription) {
            setName("Legacy Source Thread - " + taskDescription);
        }

        /**
         * @return future that is completed once this thread completes. If this task {@link
         *     #isFailing()} and this thread is not alive (e.g. not started) returns a normally
         *     completed future.
         */
        CompletableFuture<Void> getCompletionFuture() {
            return isFailing() && !isAlive()
                    ? CompletableFuture.completedFuture(null)
                    : completionFuture;
        }
    }
}
