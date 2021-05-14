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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.concurrent.ScheduledFuture;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>>
        extends AbstractUdfStreamOperator<OUT, SRC> {

    private static final long serialVersionUID = 1L;

    private transient SourceFunction.SourceContext<OUT> ctx;

    private transient volatile boolean canceledOrStopped = false;

    // 已经发送 最大的 Watermark ?
    private transient volatile boolean hasSentMaxWatermark = false;

    public StreamSource(SRC sourceFunction) {
        super(sourceFunction);

        this.chainingStrategy = ChainingStrategy.HEAD;
    }

    public void run(
            final Object lockingObject,
            final StreamStatusMaintainer streamStatusMaintainer,
            final OperatorChain<?, ?> operatorChain)
            throws Exception {

        //   streamStatusMaintainer = {OperatorChain@7182}
        //        streamOutputs = {RecordWriterOutput[1]@7320}
        //        mainOperatorOutput = {RecordWriterOutput@7306}
        //        mainOperatorWrapper = {StreamOperatorWrapper@7321}
        //        firstOperatorWrapper = {StreamOperatorWrapper@7321}
        //        tailOperatorWrapper = {StreamOperatorWrapper@7321}
        //        chainedSources = {Collections$EmptyMap@7322}  size = 0
        //        numOperators = 1
        //        operatorEventDispatcher = {OperatorEventDispatcherImpl@7323}
        //        streamStatus = {StreamStatus@7324} "StreamStatus(ACTIVE)"
        run(lockingObject, streamStatusMaintainer, output, operatorChain);
    }

    public void run(
            final Object lockingObject,
            final StreamStatusMaintainer streamStatusMaintainer,
            final Output<StreamRecord<OUT>> collector,
            final OperatorChain<?, ?> operatorChain)
            throws Exception {

        final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();

        //
        //    "internal.jobgraph-path" -> "job.graph"
        //    "high-availability.cluster-id" -> "application_1619273419318_0053"
        //    "jobmanager.rpc.address" -> "henghe-030"
        //    "taskmanager.memory.task.off-heap.size" -> "0b"
        //    "sun.security.krb5.debug" -> "true"
        //    "io.tmp.dirs" -> "/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1619273419318_0053"
        //    "parallelism.default" -> "4"
        //    "taskmanager.memory.process.size" -> "1728m"
        //    "web.port" -> "0"
        //    "jobmanager.memory.off-heap.size" -> "134217728b"
        //    "web.tmpdir" -> "/tmp/flink-web-c11e7cee-a537-425a-bf24-983e0eac5d77"
        //    "jobmanager.rpc.port" -> "35339"
        //    "security.kerberos.login.principal" -> "yarn/henghe-030@HENGHE.COM"
        //    "slot.request.timeout" -> "1000000000"
        //    "rest.address" -> "henghe-030"
        //    "security.kerberos.login.keytab" -> "/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1619273419318_0053/container_1619273419318_0053_01_000002/krb5.keytab"
        //    "$internal.deployment.config-dir" -> "/opt/tools/flink-1.12.0/conf"
        //    "$internal.yarn.log-config-file" -> "/opt/tools/flink-1.12.0/conf/log4j.properties"
        //    "jobmanager.memory.jvm-overhead.max" -> "201326592b"
        //    "jobmanager.execution.failover-strategy" -> "region"
        //    "rest.connection-timeout" -> "360000000"
        //    "taskmanager.cpu.cores" -> "4.0"
        //    "jobmanager.memory.jvm-overhead.min" -> "201326592b"
        //    "security.kerberos.login.use-ticket-cache" -> "true"
        //    "execution.savepoint.ignore-unclaimed-state" -> "false"
        //    "taskmanager.memory.framework.off-heap.size" -> "134217728b"
        //    "taskmanager.host" -> "henghe-030"
        //    "taskmanager.numberOfTaskSlots" -> "4"
        //    "taskmanager.memory.task.heap.size" -> "402653174b"
        //    "akka.ask.timeout" -> "1000000s"
        //    "akka.jvm-exit-on-fatal-error" -> {Boolean@7523} true
        //    "taskmanager.resource-id" -> "container_1619273419318_0053_01_000002"
        //    "heartbeat.timeout" -> "10000000000"
        //    "rest.idleness-timeout" -> "360000000"
        //    "taskmanager.memory.network.min" -> "134217730b"
        //    "web.timeout" -> "3600000000"
        //    "execution.target" -> "yarn-per-job"
        //    "jobmanager.memory.process.size" -> "1600m"
        //    "internal.taskmanager.resource-id.metadata" -> "henghe-030:37326"
        //    "internal.io.tmpdirs.use-local-default" -> {Boolean@7523} true
        //    "taskmanager.memory.network.max" -> "134217730b"
        //    "execution.attached" -> "true"
        //    "internal.cluster.execution-mode" -> "NORMAL"
        //    "execution.shutdown-on-attached-exit" -> "false"
        //    "pipeline.jars" -> "file:/opt/tools/flink-1.12.0/examples/streaming/SocketWindowWordCount.jar"
        //    "taskmanager.memory.managed.size" -> "536870920b"
        //    "taskmanager.memory.framework.heap.size" -> "134217728b"
        //    "env.java.opts.taskmanager" -> "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5007"
        //    "jobmanager.memory.jvm-metaspace.size" -> "268435456b"
        //    "jobmanager.memory.heap.size" -> "1073741824b"
        final Configuration configuration =
                this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();

        // 0
        final long latencyTrackingInterval =
                getExecutionConfig().isLatencyTrackingConfigured()
                        ? getExecutionConfig().getLatencyTrackingInterval()
                        : configuration.getLong(MetricOptions.LATENCY_INTERVAL);

        LatencyMarksEmitter<OUT> latencyEmitter = null;
        if (latencyTrackingInterval > 0) {
            latencyEmitter =
                    new LatencyMarksEmitter<>(
                            getProcessingTimeService(),
                            collector,
                            latencyTrackingInterval,
                            this.getOperatorID(),
                            getRuntimeContext().getIndexOfThisSubtask());
        }

        // 200
        final long watermarkInterval =
                getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

        //
        //    this.ctx = {StreamSourceContexts$ManualWatermarkContext@7657}
        //        output = {CountingOutput@7212}
        //        reuse = {StreamRecord@7658} "Record @ (undef) : null"
        //        timeService = {ProcessingTimeServiceImpl@7217}
        //        checkpointLock = {Object@7183}
        //        streamStatusMaintainer = {OperatorChain@7182}
        //        idleTimeout = -1
        //        nextCheck = null
        //        failOnNextCheck = false
        this.ctx =
                StreamSourceContexts.getSourceContext(
                        timeCharacteristic,
                        getProcessingTimeService(),
                        lockingObject,
                        streamStatusMaintainer,
                        collector,
                        watermarkInterval,
                        -1);

        try {

            //userFunction = {SocketTextStreamFunction@7209}
            // SocketTextStreamFunction#run
            userFunction.run(ctx);

            // if we get here, then the user function either exited after being done (finite source)
            // or the function was canceled or stopped. For the finite source case, we should emit
            // a final watermark that indicates that we reached the end of event-time, and end
            // inputs
            // of the operator chain
            if (!isCanceledOrStopped()) {
                // in theory, the subclasses of StreamSource may implement the BoundedOneInput
                // interface,
                // so we still need the following call to end the input
                synchronized (lockingObject) {
                    operatorChain.setIgnoreEndOfInput(false);
                    operatorChain.endInput(1);
                }
            }
        } finally {
            if (latencyEmitter != null) {
                latencyEmitter.close();
            }
        }
    }

    public void advanceToEndOfEventTime() {
        if (!hasSentMaxWatermark) {
            ctx.emitWatermark(Watermark.MAX_WATERMARK);
            hasSentMaxWatermark = true;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (!isCanceledOrStopped() && ctx != null) {
                advanceToEndOfEventTime();
            }
        } finally {
            // make sure that the context is closed in any case
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    public void cancel() {
        // important: marking the source as stopped has to happen before the function is stopped.
        // the flag that tracks this status is volatile, so the memory model also guarantees
        // the happens-before relationship
        markCanceledOrStopped();
        userFunction.cancel();

        // the context may not be initialized if the source was never running.
        if (ctx != null) {
            ctx.close();
        }
    }

    /**
     * Marks this source as canceled or stopped.
     *
     * <p>This indicates that any exit of the {@link #run(Object, StreamStatusMaintainer, Output)}
     * method cannot be interpreted as the result of a finite source.
     */
    protected void markCanceledOrStopped() {
        this.canceledOrStopped = true;
    }

    /**
     * Checks whether the source has been canceled or stopped.
     *
     * @return True, if the source is canceled or stopped, false is not.
     */
    protected boolean isCanceledOrStopped() {
        return canceledOrStopped;
    }

    private static class LatencyMarksEmitter<OUT> {
        private final ScheduledFuture<?> latencyMarkTimer;

        public LatencyMarksEmitter(
                final ProcessingTimeService processingTimeService,
                final Output<StreamRecord<OUT>> output,
                long latencyTrackingInterval,
                final OperatorID operatorId,
                final int subtaskIndex) {

            latencyMarkTimer =
                    processingTimeService.scheduleWithFixedDelay(
                            new ProcessingTimeCallback() {
                                @Override
                                public void onProcessingTime(long timestamp) throws Exception {
                                    try {
                                        // ProcessingTimeService callbacks are executed under the
                                        // checkpointing lock
                                        output.emitLatencyMarker(
                                                new LatencyMarker(
                                                        processingTimeService
                                                                .getCurrentProcessingTime(),
                                                        operatorId,
                                                        subtaskIndex));
                                    } catch (Throwable t) {
                                        // we catch the Throwables here so that we don't trigger the
                                        // processing
                                        // timer services async exception handler
                                        LOG.warn("Error while emitting latency marker.", t);
                                    }
                                }
                            },
                            0L,
                            latencyTrackingInterval);
        }

        public void close() {
            latencyMarkTimer.cancel(true);
        }
    }
}
