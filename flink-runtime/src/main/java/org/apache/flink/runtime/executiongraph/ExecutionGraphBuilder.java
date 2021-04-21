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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link
 * JobGraph}.
 */
public class ExecutionGraphBuilder {

    /**
     * Builds the ExecutionGraph from the JobGraph. If a prior execution graph exists, the JobGraph
     * will be attached. If no prior execution graph exists, then the JobGraph will become attach to
     * a new empty execution graph.
     */
    @VisibleForTesting
    public static ExecutionGraph buildGraph(
            @Nullable ExecutionGraph prior,
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            SlotProvider slotProvider,
            ClassLoader classLoader,
            CheckpointRecoveryFactory recoveryFactory,
            Time rpcTimeout,
            RestartStrategy restartStrategy,
            MetricGroup metrics,
            BlobWriter blobWriter,
            Time allocationTimeout,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            long initializationTimestamp)
            throws JobExecutionException, JobException {

        final FailoverStrategy.Factory failoverStrategy =
                FailoverStrategyLoader.loadFailoverStrategy(jobManagerConfig, log);

        return buildGraph(
                prior,
                jobGraph,
                jobManagerConfig,
                futureExecutor,
                ioExecutor,
                slotProvider,
                classLoader,
                recoveryFactory,
                rpcTimeout,
                restartStrategy,
                metrics,
                blobWriter,
                allocationTimeout,
                log,
                shuffleMaster,
                partitionTracker,
                failoverStrategy,
                NoOpExecutionDeploymentListener.get(),
                (execution, newState) -> {},
                initializationTimestamp);
    }

    public static ExecutionGraph buildGraph(
            @Nullable ExecutionGraph prior,
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            SlotProvider slotProvider,
            ClassLoader classLoader,
            CheckpointRecoveryFactory recoveryFactory,
            Time rpcTimeout,
            RestartStrategy restartStrategy,
            MetricGroup metrics,
            BlobWriter blobWriter,
            Time allocationTimeout,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            FailoverStrategy.Factory failoverStrategyFactory,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp)
            throws JobExecutionException, JobException {


            //    @Nullable ExecutionGraph prior : mull
            //    JobGraph jobGraph : JobGraph(jobId: 1aec85fe629f9f6787f0592497608304)
            //    Configuration jobManagerConfig : {internal.jobgraph-path=job.graph, env.java.opts.jobmanager=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006, jobmanager.execution.failover-strategy=region, high-availability.cluster-id=application_1617875095602_0012, jobmanager.rpc.address=henghe-030, jobmanager.memory.jvm-overhead.min=201326592b, security.kerberos.login.use-ticket-cache=true, sun.security.krb5.debug=true, execution.savepoint.ignore-unclaimed-state=false, io.tmp.dirs=/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617875095602_0012, parallelism.default=4, taskmanager.numberOfTaskSlots=1, taskmanager.memory.process.size=1728m, web.port=0, jobmanager.memory.off-heap.size=134217728b, execution.target=yarn-per-job, jobmanager.memory.process.size=1600m, web.tmpdir=/tmp/flink-web-c09ab06f-14b4-4842-92e8-9bdcd1b4a26d, jobmanager.rpc.port=43728, security.kerberos.login.principal=yarn/henghe-030@HENGHE.COM, internal.io.tmpdirs.use-local-default=true, execution.attached=true, internal.cluster.execution-mode=NORMAL, execution.shutdown-on-attached-exit=false, pipeline.jars=file:/opt/tools/flink-1.12.0/examples/streaming/SocketWindowWordCount.jar, rest.address=henghe-030, jobmanager.memory.jvm-metaspace.size=268435456b, security.kerberos.login.keytab=/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617875095602_0012/container_1617875095602_0012_01_000001/krb5.keytab, $internal.deployment.config-dir=/opt/tools/flink-1.12.0/conf, $internal.yarn.log-config-file=/opt/tools/flink-1.12.0/conf/log4j.properties, jobmanager.memory.heap.size=1073741824b, jobmanager.memory.jvm-overhead.max=201326592b}
            //    ScheduledExecutorService futureExecutor : java.util.concurrent.ScheduledThreadPoolExecutor@2e9bf748[Running, pool size = 1, active threads = 0, queued tasks = 1, completed tasks = 0]
            //    Executor ioExecutor : java.util.concurrent.ScheduledThreadPoolExecutor@2e9bf748[Running, pool size = 1, active threads = 0, queued tasks = 1, completed tasks = 0]
            //    SlotProvider slotProvider :
            //    ClassLoader classLoader :  org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader@747f7a53
            //    CheckpointRecoveryFactory recoveryFactory : StandaloneCheckpointRecoveryFactory
            //    Time rpcTimeout : 10000ms
            //    RestartStrategy restartStrategy :
            //    MetricGroup metrics :
            //    BlobWriter blobWriter : Thread[BLOB Server listener at 42491,5,main]
            //    Time allocationTimeout : 0 ms
            //    Logger log :
            //    ShuffleMaster<?> shuffleMaster :
            //    JobMasterPartitionTracker partitionTracker :
            //    FailoverStrategy.Factory failoverStrategyFactory :
            //    ExecutionDeploymentListener executionDeploymentListener :
            //    ExecutionStateUpdateListener executionStateUpdateListener :
            //    long initializationTimestamp :

        checkNotNull(jobGraph, "job graph cannot be null");

        // Socket Window WordCount
        final String jobName = jobGraph.getName();

        // 1aec85fe629f9f6787f0592497608304
        final JobID jobId = jobGraph.getJobID();

        // JobInformation for 'Socket Window WordCount' (1aec85fe629f9f6787f0592497608304)
        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths());

        // maxPriorAttemptsHistoryLength : 16
        final int maxPriorAttemptsHistoryLength =
                jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

        final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory =
                PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(
                        jobManagerConfig);

        // create a new execution graph, if none exists so far
        final ExecutionGraph executionGraph;
        try {
            // 构建ExecutionGraph
            executionGraph =
                    (prior != null)
                            ? prior
                            : new ExecutionGraph(
                                    jobInformation,
                                    futureExecutor,
                                    ioExecutor,
                                    rpcTimeout,
                                    restartStrategy,
                                    maxPriorAttemptsHistoryLength,
                                    failoverStrategyFactory,
                                    slotProvider,
                                    classLoader,
                                    blobWriter,
                                    allocationTimeout,
                                    partitionReleaseStrategyFactory,
                                    shuffleMaster,
                                    partitionTracker,
                                    jobGraph.getScheduleMode(),
                                    executionDeploymentListener,
                                    executionStateUpdateListener,
                                    initializationTimestamp);
        } catch (IOException e) {
            throw new JobException("Could not create the ExecutionGraph.", e);
        }


        // 设置基础属性 ...
        //    {
        //        "jid":"6f62d20b6ab55d63983db42b98be9e50",
        //            "name":"Socket Window WordCount",
        //            "nodes":[
        //        {
        //            "id":"6d2677a0ecc3fd8df0b72ec675edf8f4",
        //                "parallelism":1,
        //                "operator":"",
        //                "operator_strategy":"",
        //                "description":"Sink: Print to Std. Out",
        //                "inputs":[
        //            {
        //                "num":0,
        //                    "id":"ea632d67b7d595e5b851708ae9ad79d6",
        //                    "ship_strategy":"REBALANCE",
        //                    "exchange":"pipelined_bounded"
        //            }
        //        ],
        //            "optimizer_properties":{
        //
        //        }
        //        },
        //        {
        //            "id":"ea632d67b7d595e5b851708ae9ad79d6",
        //                "parallelism":4,
        //                "operator":"",
        //                "operator_strategy":"",
        //                "description":"Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction)",
        //                "inputs":[
        //            {
        //                "num":0,
        //                    "id":"0a448493b4782967b150582570326227",
        //                    "ship_strategy":"HASH",
        //                    "exchange":"pipelined_bounded"
        //            }
        //        ],
        //            "optimizer_properties":{
        //
        //        }
        //        },
        //        {
        //            "id":"0a448493b4782967b150582570326227",
        //                "parallelism":4,
        //                "operator":"",
        //                "operator_strategy":"",
        //                "description":"Flat Map",
        //                "inputs":[
        //            {
        //                "num":0,
        //                    "id":"bc764cd8ddf7a0cff126f51c16239658",
        //                    "ship_strategy":"REBALANCE",
        //                    "exchange":"pipelined_bounded"
        //            }
        //        ],
        //            "optimizer_properties":{
        //
        //        }
        //        },
        //        {
        //            "id":"bc764cd8ddf7a0cff126f51c16239658",
        //                "parallelism":1,
        //                "operator":"",
        //                "operator_strategy":"",
        //                "description":"Source: Socket Stream",
        //                "optimizer_properties":{
        //
        //        }
        //        }
        //]
        //    }
        // set the basic properties

        try {





            // {"jid":"6f62d20b6ab55d63983db42b98be9e50","name":"Socket Window WordCount","nodes":[{"id":"6d2677a0ecc3fd8df0b72ec675edf8f4","parallelism":1,"operator":"","operator_strategy":"","description":"Sink: Print to Std. Out","inputs":[{"num":0,"id":"ea632d67b7d595e5b851708ae9ad79d6","ship_strategy":"REBALANCE","exchange":"pipelined_bounded"}],"optimizer_properties":{}},{"id":"ea632d67b7d595e5b851708ae9ad79d6","parallelism":4,"operator":"","operator_strategy":"","description":"Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction)","inputs":[{"num":0,"id":"0a448493b4782967b150582570326227","ship_strategy":"HASH","exchange":"pipelined_bounded"}],"optimizer_properties":{}},{"id":"0a448493b4782967b150582570326227","parallelism":4,"operator":"","operator_strategy":"","description":"Flat Map","inputs":[{"num":0,"id":"bc764cd8ddf7a0cff126f51c16239658","ship_strategy":"REBALANCE","exchange":"pipelined_bounded"}],"optimizer_properties":{}},{"id":"bc764cd8ddf7a0cff126f51c16239658","parallelism":1,"operator":"","operator_strategy":"","description":"Source: Socket Stream","optimizer_properties":{}}]}
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);


        //
        //    taskVertices = {LinkedHashMap@7348}  size = 4
        //        {JobVertexID@7361} "6d2677a0ecc3fd8df0b72ec675edf8f4" -> {JobVertex@7313} "Sink: Print to Std. Out (org.apache.flink.streaming.runtime.tasks.OneInputStreamTask)"
        //        {JobVertexID@7362} "ea632d67b7d595e5b851708ae9ad79d6" -> {JobVertex@7363} "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) (org.apache.flink.streaming.runtime.tasks.OneInputStreamTask)"
        //        {JobVertexID@7364} "0a448493b4782967b150582570326227" -> {JobVertex@7365} "Flat Map (org.apache.flink.streaming.runtime.tasks.OneInputStreamTask)"
        //        {JobVertexID@7366} "bc764cd8ddf7a0cff126f51c16239658" -> {JobVertex@7367} "Source: Socket Stream (org.apache.flink.streaming.runtime.tasks.SourceStreamTask)"

        for (JobVertex vertex : jobGraph.getVertices()) {

            // org.apache.flink.streaming.runtime.tasks.OneInputStreamTask
            // org.apache.flink.streaming.runtime.tasks.OneInputStreamTask
            // org.apache.flink.streaming.runtime.tasks.OneInputStreamTask
            // org.apache.flink.streaming.runtime.tasks.SourceStreamTask

            String executableClass = vertex.getInvokableClassName();
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            try {
                // org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader@3131d8fc





                vertex.initializeOnMaster(classLoader);
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }

        log.info(
                "Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        // job vertices 拓扑排序
        // 对作业顶点进行拓扑排序，然后将图形附加到现有的顶点上 . 获取所有的JobVertex
        // topologically sort the job vertices and attach the graph to the existing one

        //    0 = {JobVertex@7480} "Source: Socket Stream (org.apache.flink.streaming.runtime.tasks.SourceStreamTask)"
        //    1 = {JobVertex@7459} "Flat Map (org.apache.flink.streaming.runtime.tasks.OneInputStreamTask)"
        //    2 = {JobVertex@7448} "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) (org.apache.flink.streaming.runtime.tasks.OneInputStreamTask)"
        //    3 = {JobVertex@7426} "Sink: Print to Std. Out (org.apache.flink.streaming.runtime.tasks.OneInputStreamTask)"
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId);
        }

        // [重点]
        executionGraph.attachJobGraph(sortedTopology);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        // configure the state checkpointing
        JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
        if (snapshotSettings != null) {
            List<ExecutionJobVertex> triggerVertices =
                    idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

            List<ExecutionJobVertex> ackVertices =
                    idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

            List<ExecutionJobVertex> confirmVertices =
                    idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

            CompletedCheckpointStore completedCheckpoints;
            CheckpointIDCounter checkpointIdCounter;
            try {
                int maxNumberOfCheckpointsToRetain =
                        jobManagerConfig.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

                if (maxNumberOfCheckpointsToRetain <= 0) {
                    // warning and use 1 as the default value if the setting in
                    // state.checkpoints.max-retained-checkpoints is not greater than 0.
                    log.warn(
                            "The setting for '{} : {}' is invalid. Using default value of {}",
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                            maxNumberOfCheckpointsToRetain,
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

                    maxNumberOfCheckpointsToRetain =
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
                }

                completedCheckpoints =
                        recoveryFactory.createCheckpointStore(
                                jobId, maxNumberOfCheckpointsToRetain, classLoader);
                checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
            } catch (Exception e) {
                throw new JobExecutionException(
                        jobId, "Failed to initialize high-availability checkpoint handler", e);
            }

            // Maximum number of remembered checkpoints
            int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

            CheckpointStatsTracker checkpointStatsTracker =
                    new CheckpointStatsTracker(
                            historySize,
                            ackVertices,
                            snapshotSettings.getCheckpointCoordinatorConfiguration(),
                            metrics);

            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend, jobManagerConfig, classLoader, log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // instantiate the user-defined checkpoint hooks

            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();

            executionGraph.enableCheckpointing(
                    chkConfig,
                    triggerVertices,
                    ackVertices,
                    confirmVertices,
                    hooks,
                    checkpointIdCounter,
                    completedCheckpoints,
                    rootBackend,
                    checkpointStatsTracker);
        }

        // create all the metrics for the Execution Graph

        metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));

        executionGraph.getFailoverStrategy().registerMetrics(metrics);

        return executionGraph;
    }

    private static List<ExecutionJobVertex> idToVertex(
            List<JobVertexID> jobVertices, ExecutionGraph executionGraph)
            throws IllegalArgumentException {

        List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

        for (JobVertexID id : jobVertices) {
            ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
            if (vertex != null) {
                result.add(vertex);
            } else {
                throw new IllegalArgumentException(
                        "The snapshot checkpointing settings refer to non-existent vertex " + id);
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private ExecutionGraphBuilder() {}
}
