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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointType.PostCheckpointAction;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionInfo;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcKvStateRegistryListener;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.rpc.RpcTaskOperatorEventGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotActions;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotActiveException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlot;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ThreadInfo;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 */
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    public static final String TASK_MANAGER_NAME = "taskmanager";

    // HA
    /** The access to the leader election and retrieval services. */
    private final HighAvailabilityServices haServices;

    // TaskExecutor 相关的服务比如: MemoryManager , IOManager ,ShuffleEnvironment 等等
    private final TaskManagerServices taskExecutorServices;

    /**
     * The task manager configuration.
     * */
    private final TaskManagerConfiguration taskManagerConfiguration;

    /** The fatal error handler to use in case of a fatal error. */
    private final FatalErrorHandler fatalErrorHandler;

    // BLOB缓存提供对永久和临时BLOB的BLOB服务的访问。
    private final BlobCacheService blobCacheService;

    private final LibraryCacheManager libraryCacheManager;

    /** The address to metric query service on this Task Manager. */
    @Nullable private final String metricQueryServiceAddress;

    // --------- TaskManager services --------

    /**
     * 此任务管理器的连接信息。
     * The connection information of this task manager. */
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private final TaskManagerMetricGroup taskManagerMetricGroup;

    /**
     * 此任务的状态管理器，为每个插槽提供状态管理器。
     * The state manager for this task, providing state managers per slot. */
    private final TaskExecutorLocalStateStoresManager localStateStoresManager;

    /** Information provider for external resources. */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The network component in the task manager. */
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    /** The kvState registration service in the task manager. */
    private final KvStateService kvStateService;

    private final Executor ioExecutor;

    // --------- task slot allocation table -----------

    private final TaskSlotTable<Task> taskSlotTable;

    private final JobTable jobTable;

    private final JobLeaderService jobLeaderService;

    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // ------------------------------------------------------------------------

    // 硬件描述信息
    private final HardwareDescription hardwareDescription;

    // 内存配置信息
    private final TaskExecutorMemoryConfiguration memoryConfiguration;

    // 文件缓存
    private FileCache fileCache;

    // jobManager 心跳相关
    /** The heartbeat manager for job manager in the task manager. */
    private final HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload>
            jobManagerHeartbeatManager;

    // resource manager 心跳相关
    /** The heartbeat manager for resource manager in the task manager. */
    private final HeartbeatManager<Void, TaskExecutorHeartbeatPayload>
            resourceManagerHeartbeatManager;

    // 分区相关
    private final TaskExecutorPartitionTracker partitionTracker;

    // 背压相关
    private final BackPressureSampleService backPressureSampleService;

    // --------- resource manager --------

    // resource manager 相关
    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    @Nullable private TaskExecutorToResourceManagerConnection resourceManagerConnection;

    @Nullable private UUID currentRegistrationTimeoutId;

    private Map<JobID, Collection<CompletableFuture<ExecutionState>>>
            taskResultPartitionCleanupFuturesPerJob = new HashMap<>(8);

    public TaskExecutor(
            RpcService rpcService,
            TaskManagerConfiguration taskManagerConfiguration,
            HighAvailabilityServices haServices,
            TaskManagerServices taskExecutorServices,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            HeartbeatServices heartbeatServices,
            TaskManagerMetricGroup taskManagerMetricGroup,
            @Nullable String metricQueryServiceAddress,
            BlobCacheService blobCacheService,
            FatalErrorHandler fatalErrorHandler,
            TaskExecutorPartitionTracker partitionTracker,
            BackPressureSampleService backPressureSampleService) {

        super(rpcService, AkkaRpcServiceUtils.createRandomName(TASK_MANAGER_NAME));

        checkArgument(
                taskManagerConfiguration.getNumberSlots() > 0,
                "The number of slots has to be larger than 0.");

        this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
        this.taskExecutorServices = checkNotNull(taskExecutorServices);
        this.haServices = checkNotNull(haServices);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.partitionTracker = partitionTracker;
        this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
        this.blobCacheService = checkNotNull(blobCacheService);
        this.metricQueryServiceAddress = metricQueryServiceAddress;
        this.backPressureSampleService = checkNotNull(backPressureSampleService);
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.libraryCacheManager = taskExecutorServices.getLibraryCacheManager();
        this.taskSlotTable = taskExecutorServices.getTaskSlotTable();
        this.jobTable = taskExecutorServices.getJobTable();
        this.jobLeaderService = taskExecutorServices.getJobLeaderService();
        this.unresolvedTaskManagerLocation =
                taskExecutorServices.getUnresolvedTaskManagerLocation();
        this.localStateStoresManager = taskExecutorServices.getTaskManagerStateStore();
        this.shuffleEnvironment = taskExecutorServices.getShuffleEnvironment();
        this.kvStateService = taskExecutorServices.getKvStateService();
        this.ioExecutor = taskExecutorServices.getIOExecutor();
        this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();

        this.hardwareDescription =
                HardwareDescription.extractFromSystem(taskExecutorServices.getManagedMemorySize());
        this.memoryConfiguration =
                TaskExecutorMemoryConfiguration.create(taskManagerConfiguration.getConfiguration());

        this.resourceManagerAddress = null;
        this.resourceManagerConnection = null;
        this.currentRegistrationTimeoutId = null;

        final ResourceID resourceId =
                taskExecutorServices.getUnresolvedTaskManagerLocation().getResourceID();
        this.jobManagerHeartbeatManager =
                createJobManagerHeartbeatManager(heartbeatServices, resourceId);
        this.resourceManagerHeartbeatManager =
                createResourceManagerHeartbeatManager(heartbeatServices, resourceId);
    }

    private HeartbeatManager<Void, TaskExecutorHeartbeatPayload>
            createResourceManagerHeartbeatManager(
                    HeartbeatServices heartbeatServices, ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload>
            createJobManagerHeartbeatManager(
                    HeartbeatServices heartbeatServices, ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new JobManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return CompletableFuture.completedFuture(
                shuffleEnvironment.getPartitionsOccupyingLocalResources().isEmpty());
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
        return CompletableFuture.supplyAsync(
                () -> {
                    final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
                    if (logDir != null) {
                        final File[] logFiles = new File(logDir).listFiles();

                        if (logFiles == null) {
                            throw new CompletionException(
                                    new FlinkException(
                                            String.format(
                                                    "There isn't a log file in TaskExecutor’s log dir %s.",
                                                    logDir)));
                        }

                        return Arrays.stream(logFiles)
                                .filter(File::isFile)
                                .map(logFile -> new LogInfo(logFile.getName(), logFile.length()))
                                .collect(Collectors.toList());
                    }
                    return Collections.emptyList();
                },
                ioExecutor);
    }

    // ------------------------------------------------------------------------
    //  Life cycle
    //  TaskExecutor 方法启动
    // ------------------------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            // 启动服务...
            startTaskExecutorServices();
        } catch (Throwable t) {
            final TaskManagerException exception =
                    new TaskManagerException(
                            String.format("Could not start the TaskExecutor %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }

        startRegistrationTimeout();
    }

    // 启动 TaskExecutor 服务.
    private void startTaskExecutorServices() throws Exception {
        try {
            // start by connecting to the ResourceManager
            // 连接 ResourceManager .   StandaloneLeaderRetrievalService#Start
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

            // tell the task slot table who's responsible for the task slot actions
            // 告诉 task slot table 谁负责 task slot 操作
            taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());

            // start the job leader service
            jobLeaderService.start(
                    getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

            fileCache =
                    new FileCache(
                            taskManagerConfiguration.getTmpDirectories(),
                            blobCacheService.getPermanentBlobService());
        } catch (Exception e) {
            handleStartTaskExecutorServicesException(e);
        }
    }

    private void handleStartTaskExecutorServicesException(Exception e) throws Exception {
        try {
            stopTaskExecutorServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    /** Called to shut down the TaskManager. The method closes all TaskManager services. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping TaskExecutor {}.", getAddress());

        Throwable jobManagerDisconnectThrowable = null;

        FlinkException cause = new FlinkException("The TaskExecutor is shutting down.");

        closeResourceManagerConnection(cause);

        for (JobTable.Job job : jobTable.getJobs()) {
            try {
                closeJob(job, cause);
            } catch (Throwable t) {
                jobManagerDisconnectThrowable =
                        ExceptionUtils.firstOrSuppressed(t, jobManagerDisconnectThrowable);
            }
        }

        Preconditions.checkState(jobTable.isEmpty());

        final Throwable throwableBeforeTasksCompletion = jobManagerDisconnectThrowable;

        return FutureUtils.runAfterwards(taskSlotTable.closeAsync(), this::stopTaskExecutorServices)
                .handle(
                        (ignored, throwable) -> {
                            handleOnStopException(throwableBeforeTasksCompletion, throwable);
                            return null;
                        });
    }

    private void handleOnStopException(
            Throwable throwableBeforeTasksCompletion, Throwable throwableAfterTasksCompletion) {
        final Throwable throwable;

        if (throwableBeforeTasksCompletion != null) {
            throwable =
                    ExceptionUtils.firstOrSuppressed(
                            throwableBeforeTasksCompletion, throwableAfterTasksCompletion);
        } else {
            throwable = throwableAfterTasksCompletion;
        }

        if (throwable != null) {
            throw new CompletionException(
                    new FlinkException("Error while shutting the TaskExecutor down.", throwable));
        } else {
            log.info("Stopped TaskExecutor {}.", getAddress());
        }
    }

    private void stopTaskExecutorServices() throws Exception {
        Exception exception = null;

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskExecutorServices.shutDown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            fileCache.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        // it will call close() recursively from the parent to children
        taskManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ======================================================================
    //  RPC methods
    // ======================================================================

    @Override
    public CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
            ExecutionAttemptID executionAttemptId, int requestId, @RpcTimeout Time timeout) {

        final Task task = taskSlotTable.getTask(executionAttemptId);
        if (task == null) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(
                            String.format(
                                    "Cannot request back pressure of task %s. "
                                            + "Task is not known to the task manager.",
                                    executionAttemptId)));
        }
        final CompletableFuture<Double> backPressureRatioFuture =
                backPressureSampleService.sampleTaskBackPressure(task);

        return backPressureRatioFuture.thenApply(
                backPressureRatio ->
                        new TaskBackPressureResponse(
                                requestId, executionAttemptId, backPressureRatio));
    }

    // ----------------------------------------------------------------------
    // Task lifecycle RPCs
    // 提交 任务 !!!
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {

        try {
            final JobID jobId = tdd.getJobId();
            final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

            final JobTable.Connection jobManagerConnection =
                    jobTable.getConnection(jobId)
                            .orElseThrow(
                                    () -> {
                                        final String message =
                                                "Could not submit task because there is no JobManager "
                                                        + "associated for the job "
                                                        + jobId
                                                        + '.';

                                        log.debug(message);
                                        return new TaskSubmissionException(message);
                                    });

            if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
                final String message =
                        "Rejecting the task submission because the job manager leader id "
                                + jobMasterId
                                + " does not match the expected job manager leader id "
                                + jobManagerConnection.getJobMasterId()
                                + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
                final String message =
                        "No task slot allocated for job ID "
                                + jobId
                                + " and allocation ID "
                                + tdd.getAllocationId()
                                + '.';
                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            // re-integrate offloaded data:
            try {
                tdd.loadBigData(blobCacheService.getPermanentBlobService());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
            }

            // deserialize the pre-serialized information
            final JobInformation jobInformation;
            final TaskInformation taskInformation;
            try {
                jobInformation =
                        tdd.getSerializedJobInformation()
                                .deserializeValue(getClass().getClassLoader());
                taskInformation =
                        tdd.getSerializedTaskInformation()
                                .deserializeValue(getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not deserialize the job or task information.", e);
            }

            if (!jobId.equals(jobInformation.getJobId())) {
                throw new TaskSubmissionException(
                        "Inconsistent job ID information inside TaskDeploymentDescriptor ("
                                + tdd.getJobId()
                                + " vs. "
                                + jobInformation.getJobId()
                                + ")");
            }

            TaskMetricGroup taskMetricGroup =
                    taskManagerMetricGroup.addTaskForJob(
                            jobInformation.getJobId(),
                            jobInformation.getJobName(),
                            taskInformation.getJobVertexId(),
                            tdd.getExecutionAttemptId(),
                            taskInformation.getTaskName(),
                            tdd.getSubtaskIndex(),
                            tdd.getAttemptNumber());

            InputSplitProvider inputSplitProvider =
                    new RpcInputSplitProvider(
                            jobManagerConnection.getJobManagerGateway(),
                            taskInformation.getJobVertexId(),
                            tdd.getExecutionAttemptId(),
                            taskManagerConfiguration.getTimeout());

            final TaskOperatorEventGateway taskOperatorEventGateway =
                    new RpcTaskOperatorEventGateway(
                            jobManagerConnection.getJobManagerGateway(),
                            executionAttemptID,
                            (t) -> runAsync(() -> failTask(executionAttemptID, t)));

            TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
            CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
            GlobalAggregateManager aggregateManager =
                    jobManagerConnection.getGlobalAggregateManager();

            LibraryCacheManager.ClassLoaderHandle classLoaderHandle =
                    jobManagerConnection.getClassLoaderHandle();
            ResultPartitionConsumableNotifier resultPartitionConsumableNotifier =
                    jobManagerConnection.getResultPartitionConsumableNotifier();
            PartitionProducerStateChecker partitionStateChecker =
                    jobManagerConnection.getPartitionStateChecker();

            final TaskLocalStateStore localStateStore =
                    localStateStoresManager.localStateStoreForSubtask(
                            jobId,
                            tdd.getAllocationId(),
                            taskInformation.getJobVertexId(),
                            tdd.getSubtaskIndex());

            final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

            // 构造 TaskStateManager
            final TaskStateManager taskStateManager =
                    new TaskStateManagerImpl(
                            jobId,
                            tdd.getExecutionAttemptId(),
                            localStateStore,
                            taskRestore,
                            checkpointResponder);

            MemoryManager memoryManager;
            try {
                memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
            } catch (SlotNotFoundException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            // 构造一个新的Task
            Task task =
                    new Task(
                            jobInformation,
                            taskInformation,
                            tdd.getExecutionAttemptId(),
                            tdd.getAllocationId(),
                            tdd.getSubtaskIndex(),
                            tdd.getAttemptNumber(),
                            tdd.getProducedPartitions(),
                            tdd.getInputGates(),
                            tdd.getTargetSlotNumber(),
                            memoryManager,
                            taskExecutorServices.getIOManager(),
                            taskExecutorServices.getShuffleEnvironment(),
                            taskExecutorServices.getKvStateService(),
                            taskExecutorServices.getBroadcastVariableManager(),
                            taskExecutorServices.getTaskEventDispatcher(),
                            externalResourceInfoProvider,
                            taskStateManager,
                            taskManagerActions,
                            inputSplitProvider,
                            checkpointResponder,
                            taskOperatorEventGateway,
                            aggregateManager,
                            classLoaderHandle,
                            fileCache,
                            taskManagerConfiguration,
                            taskMetricGroup,
                            resultPartitionConsumableNotifier,
                            partitionStateChecker,
                            getRpcService().getExecutor());

            taskMetricGroup.gauge(MetricNames.IS_BACKPRESSURED, task::isBackPressured);

            // Received task
            //      Window(
            //          TumblingProcessingTimeWindows(5000),
            //          ProcessingTimeTrigger,
            //          ReduceFunction$1, PassThroughWindowFunction
            //      ) ->
            //      Sink: Print to Std. Out (1/1)#0 (141dd597dc560a831b2b4bc195943f0b),
            //
            // deploy into slot with allocation id
            //      3755cb8f9962a9a7738db04f2a02084c.

            log.info(
                    "Received task {} ({}), deploy into slot with allocation id {}.",
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    tdd.getExecutionAttemptId(),
                    tdd.getAllocationId());

            boolean taskAdded;

            try {
                taskAdded = taskSlotTable.addTask(task);
            } catch (SlotNotFoundException | SlotNotActiveException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            if (taskAdded) {
                // 启动线程
                task.startTaskThread();

                setupResultPartitionBookkeeping(
                        tdd.getJobId(), tdd.getProducedPartitions(), task.getTerminationFuture());
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                final String message =
                        "TaskManager already contains a task for id " + task.getExecutionId() + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }
        } catch (TaskSubmissionException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    private void setupResultPartitionBookkeeping(
            JobID jobId,
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions,
            CompletableFuture<ExecutionState> terminationFuture) {
        final Set<ResultPartitionID> partitionsRequiringRelease =
                filterPartitionsRequiringRelease(producedResultPartitions)
                        .peek(
                                rpdd ->
                                        partitionTracker.startTrackingPartition(
                                                jobId, TaskExecutorPartitionInfo.from(rpdd)))
                        .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                        .map(ShuffleDescriptor::getResultPartitionID)
                        .collect(Collectors.toSet());

        final CompletableFuture<ExecutionState> taskTerminationWithResourceCleanupFuture =
                terminationFuture.thenApplyAsync(
                        executionState -> {
                            if (executionState != ExecutionState.FINISHED) {
                                partitionTracker.stopTrackingPartitions(partitionsRequiringRelease);
                            }
                            return executionState;
                        },
                        getMainThreadExecutor());

        taskResultPartitionCleanupFuturesPerJob.compute(
                jobId,
                (ignored, completableFutures) -> {
                    if (completableFutures == null) {
                        completableFutures = new ArrayList<>(4);
                    }

                    completableFutures.add(taskTerminationWithResourceCleanupFuture);
                    return completableFutures;
                });
    }

    private Stream<ResultPartitionDeploymentDescriptor> filterPartitionsRequiringRelease(
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions) {
        return producedResultPartitions.stream()
                // only blocking partitions require explicit release call
                .filter(d -> d.getPartitionType().isBlocking())
                // partitions without local resources don't store anything on the TaskExecutor
                .filter(d -> d.getShuffleDescriptor().storesLocalResourcesOn().isPresent());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try {
                task.cancelExecution();
                return CompletableFuture.completedFuture(Acknowledge.get());
            } catch (Throwable t) {
                return FutureUtils.completedExceptionally(
                        new TaskException(
                                "Cannot cancel task for execution " + executionAttemptID + '.', t));
            }
        } else {
            final String message =
                    "Cannot find task to stop for execution " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskException(message));
        }
    }

    // ----------------------------------------------------------------------
    // Partition lifecycle RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            final ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Time timeout) {
        // 获取任务
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {

            // 迭代分区信息
            for (final PartitionInfo partitionInfo : partitionInfos) {
                // Run asynchronously because it might be blocking
                FutureUtils.assertNoException(
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        // 更改分区信息
                                        if (!shuffleEnvironment.updatePartitionInfo( executionAttemptID, partitionInfo)) {
                                            log.debug(
                                                    "Discard update for input gate partition {} of result {} in task {}. "
                                                            + "The partition is no longer available.",
                                                    partitionInfo
                                                            .getShuffleDescriptor()
                                                            .getResultPartitionID(),
                                                    partitionInfo.getIntermediateDataSetID(),
                                                    executionAttemptID);
                                        }
                                    } catch (IOException | InterruptedException e) {
                                        log.error(
                                                "Could not update input data location for task {}. Trying to fail task.",
                                                task.getTaskInfo().getTaskName(),
                                                e);
                                        task.failExternally(e);
                                    }
                                },
                                getRpcService().getExecutor()));
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            log.debug(
                    "Discard update for input partitions of task {}. Task is no longer running.",
                    executionAttemptID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    @Override
    public void releaseOrPromotePartitions(
            JobID jobId,
            Set<ResultPartitionID> partitionToRelease,
            Set<ResultPartitionID> partitionsToPromote) {
        try {
            partitionTracker.stopTrackingAndReleaseJobPartitions(partitionToRelease);
            partitionTracker.promoteJobPartitions(partitionsToPromote);

            closeJobManagerConnectionIfNoAllocatedResources(jobId);
        } catch (Throwable t) {
            // TODO: Do we still need this catch branch?
            onFatalError(t);
        }

        // TODO: Maybe it's better to return an Acknowledge here to notify the JM about the
        // success/failure with an Exception
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease, Time timeout) {
        partitionTracker.stopTrackingAndReleaseClusterPartitions(dataSetsToRelease);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------
    // Heartbeat RPC
    // ----------------------------------------------------------------------

    @Override
    public void heartbeatFromJobManager(
            ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
        jobManagerHeartbeatManager.requestHeartbeat(resourceID, allocatedSlotReport);
    }

    @Override
    public void heartbeatFromResourceManager(ResourceID resourceID) {
        resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    // ----------------------------------------------------------------------
    // Checkpointing RPCs
    // ----------------------------------------------------------------------

    // JobManager里面的 ExecutionVertex 的 Execution 触发
    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        log.debug(
                "Trigger checkpoint {}@{} for {}.",
                checkpointId,
                checkpointTimestamp,
                executionAttemptID);

        // 获取CheckpointType
        final CheckpointType checkpointType = checkpointOptions.getCheckpointType();


        if (checkpointType.getPostCheckpointAction() == PostCheckpointAction.TERMINATE
                && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
            throw new IllegalArgumentException(
                    "Only synchronous savepoints are allowed to advance the watermark to MAX.");
        }

        // 获取任务
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            // 调用task的triggerCheckpointBarrier方法, 触发chckpoint
            task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager received a checkpoint request for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(
            ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
        log.debug(
                "Confirm checkpoint {}@{} for {}.",
                checkpointId,
                checkpointTimestamp,
                executionAttemptID);

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            // 通过task的 notifyCheckpointComplete 方法 . Checkpoint完成
            task.notifyCheckpointComplete(checkpointId);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager received a checkpoint confirmation for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message,
                            CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(
            ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
        log.debug(
                "Abort checkpoint {}@{} for {}.",
                checkpointId,
                checkpointTimestamp,
                executionAttemptID);

        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            // abortCheckpoint
            task.notifyCheckpointAborted(checkpointId);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            final String message =
                    "TaskManager received an aborted checkpoint for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message,
                            CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    // ----------------------------------------------------------------------
    // Slot allocation RPCs
    //
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> requestSlot(
            final SlotID slotId,
            final JobID jobId,
            final AllocationID allocationId,
            final ResourceProfile resourceProfile,
            final String targetAddress,
            final ResourceManagerId resourceManagerId,
            final Time timeout) {
        // TODO: Filter invalid requests from the resource manager by using the
        // instance/registration Id

        //    slotId = {SlotID@6055} "container_1619273419318_0032_01_000002_0"
        //          resourceId = {ResourceID@6114} "container_1619273419318_0032_01_000002"
        //          slotNumber = 0
        //    jobId = {JobID@6056} "05fdf1bc744b274be1525c918c1ad378"
        //    allocationId = {AllocationID@6057} "a9ce7abc6f1d6f264dbdce5564efcb76"
        //    resourceProfile = {ResourceProfile@6058} "ResourceProfile{UNKNOWN}"
        //          cpuCores = null
        //          taskHeapMemory = null
        //          taskOffHeapMemory = null
        //          managedMemory = null
        //          networkMemory = null
        //    extendedResources = {HashMap@6116}  size = 0
        //    targetAddress = "akka.tcp://flink@master01:36345/user/rpc/jobmanager_2"
        //    resourceManagerId = {ResourceManagerId@6060} "00000000000000000000000000000000"
        //    timeout = {Time@6061} "1000000000 ms"



        // 输出日志信息
        // Receive slot request
        //      3755cb8f9962a9a7738db04f2a02084c
        // for job
        //      694474d11da6100e82744c9e47e2f511
        // from resource manager with leader id
        //      00000000000000000000000000000000.

        log.info(
                "Receive slot request {} for job {} from resource manager with leader id {}.",
                allocationId,
                jobId,
                resourceManagerId);


        // 是否连接到 ResourceManager
        if (!isConnectedToResourceManager(resourceManagerId)) {
            final String message =
                    String.format(
                            "TaskManager is not connected to the resource manager %s.",
                            resourceManagerId);
            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskManagerException(message));
        }

        try {

            //[重点] 分配 slot
            allocateSlot(slotId, jobId, allocationId, resourceProfile);
        } catch (SlotAllocationException sae) {
            return FutureUtils.completedExceptionally(sae);
        }

        final JobTable.Job job;

        try {

            // 获取/构建  JobTable.Job


            job =jobTable.getOrCreateJob(jobId, () -> registerNewJobAndCreateServices(jobId, targetAddress));



        } catch (Exception e) {
            // free the allocated slot
            try {
                taskSlotTable.freeSlot(allocationId);
            } catch (SlotNotFoundException slotNotFoundException) {
                // slot no longer existent, this should actually never happen, because we've
                // just allocated the slot. So let's fail hard in this case!
                onFatalError(slotNotFoundException);
            }

            // release local state under the allocation id.
            localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

            // sanity check
            if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
                onFatalError(new Exception("Could not free slot " + slotId));
            }

            return FutureUtils.completedExceptionally(
                    new SlotAllocationException("Could not create new job.", e));
        }

        if (job.isConnected()) {

            //[重要]  向JobManager提供Slot
            offerSlotsToJobManager(jobId);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private TaskExecutorJobServices registerNewJobAndCreateServices(
            JobID jobId, String targetAddress) throws Exception {
        jobLeaderService.addJob(jobId, targetAddress);
        final PermanentBlobCache permanentBlobService = blobCacheService.getPermanentBlobService();
        permanentBlobService.registerJob(jobId);

        return TaskExecutorJobServices.create(
                libraryCacheManager.registerClassLoaderLease(jobId),
                () -> permanentBlobService.releaseJob(jobId));
    }

    private void allocateSlot(
            SlotID slotId, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile)
            throws SlotAllocationException {

        //    slotId = {SlotID@6055} "container_1619273419318_0032_01_000002_0"
        //        resourceId = {ResourceID@6114} "container_1619273419318_0032_01_000002"
        //        slotNumber = 0
        //    jobId = {JobID@6056} "05fdf1bc744b274be1525c918c1ad378"
        //    allocationId = {AllocationID@6057} "a9ce7abc6f1d6f264dbdce5564efcb76"
        //    resourceProfile = {ResourceProfile@6058} "ResourceProfile{UNKNOWN}"
        //        cpuCores = null
        //        taskHeapMemory = null
        //        taskOffHeapMemory = null
        //        managedMemory = null
        //        networkMemory = null
        //    extendedResources = {HashMap@6116}  size = 0



        //    taskSlotTable = {TaskSlotTableImpl@6077}
        //    numberSlots = 4
        //        defaultSlotResourceProfile = {ResourceProfile@6124} "ResourceProfile{cpuCores=1.0000000000000000, taskHeapMemory=96.000mb (100663293 bytes), taskOffHeapMemory=0 bytes, managedMemory=128.000mb (134217730 bytes), networkMemory=32.000mb (33554432 bytes)}"
        //        cpuCores = {CPUResource@6139} "Resource(CPU: 1.0000000000000000)"
        //        taskHeapMemory = {MemorySize@6140} "100663293 bytes"
        //        taskOffHeapMemory = {MemorySize@6141} "0 bytes"
        //        managedMemory = {MemorySize@6142} "134217730 bytes"
        //        networkMemory = {MemorySize@6143} "32 mb"
        //        extendedResources = {HashMap@6144}  size = 0
        //    memoryPageSize = 32768
        //    timerService = {TimerService@6125}
        //    taskSlots = {HashMap@6126}  size = 0
        //    allocatedSlots = {HashMap@6127}  size = 0
        //    taskSlotMappings = {HashMap@6128}  size = 0
        //    slotsPerJob = {HashMap@6129}  size = 0
        //    slotActions = {TaskExecutor$SlotActionsImpl@6130}
        //    state = {TaskSlotTableImpl$State@6131} "RUNNING"
        //    budgetManager = {ResourceBudgetManager@6132}
        //    closingFuture = {CompletableFuture@6133} "java.util.concurrent.CompletableFuture@9a6e076[Not completed]"
        //    mainThreadExecutor = {RpcEndpoint$MainThreadExecutor@6096}
        //    memoryVerificationExecutor = {ThreadPoolExecutor@6076} "java.util.concurrent.ThreadPoolExecutor@da5c1a9[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]"
        //    if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {


        if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {


            // 进行分配操作..
            // TaskSlotTableImpl # allocateSlot
            if (taskSlotTable.allocateSlot(
                    slotId.getSlotNumber(),
                    jobId,
                    allocationId,
                    resourceProfile,
                    taskManagerConfiguration.getTimeout())) {


                // Allocated slot for 3755cb8f9962a9a7738db04f2a02084c.
                log.info("Allocated slot for {}.", allocationId);
            } else {
                log.info("Could not allocate slot for {}.", allocationId);
                throw new SlotAllocationException("Could not allocate slot.");
            }
        } else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
            final String message =
                    "The slot " + slotId + " has already been allocated for a different job.";

            log.info(message);

            final AllocationID allocationID =
                    taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
            throw new SlotOccupiedException(
                    message, allocationID, taskSlotTable.getOwningJob(allocationID));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Time timeout) {
        freeSlotInternal(allocationId, cause);

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(
            FileType fileType, Time timeout) {
        final String filePath;
        switch (fileType) {
            case LOG:
                filePath = taskManagerConfiguration.getTaskManagerLogPath();
                break;
            case STDOUT:
                filePath = taskManagerConfiguration.getTaskManagerStdoutPath();
                break;
            default:
                filePath = null;
        }
        return requestFileUploadByFilePath(filePath, fileType.toString());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(
            String fileName, Time timeout) {
        final String filePath;
        final String logDir = taskManagerConfiguration.getTaskManagerLogDir();

        if (StringUtils.isNullOrWhitespaceOnly(logDir)
                || StringUtils.isNullOrWhitespaceOnly(fileName)) {
            filePath = null;
        } else {
            //  根据 taskManagerLogDir + 文件的名字 获取路径
            filePath = new File(logDir, new File(fileName).getName()).getPath();
        }
        return requestFileUploadByFilePath(filePath, fileName);
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(
            Time timeout) {
        return CompletableFuture.completedFuture(
                SerializableOptional.ofNullable(metricQueryServiceAddress));
    }

    // ----------------------------------------------------------------------
    // Disconnection RPCs
    // ----------------------------------------------------------------------

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        jobTable.getConnection(jobId)
                .ifPresent(
                        jobManagerConnection ->
                                disconnectAndTryReconnectToJobManager(jobManagerConnection, cause));
    }

    private void disconnectAndTryReconnectToJobManager(
            JobTable.Connection jobManagerConnection, Exception cause) {
        disconnectJobManagerConnection(jobManagerConnection, cause);
        jobLeaderService.reconnect(jobManagerConnection.getJobId());
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        if (isRunning()) {
            reconnectToResourceManager(cause);
        }
    }

    // ----------------------------------------------------------------------
    // Other RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID executionAttemptID,
            OperatorID operatorId,
            SerializedValue<OperatorEvent> evt) {

        log.debug("Operator event for {} - {}", executionAttemptID, operatorId);

        // 获取Task
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if (task == null) {
            return FutureUtils.completedExceptionally(
                    new TaskNotRunningException(
                            "Task " + executionAttemptID + " not running on TaskManager"));
        }

        try {
            // 发送 OperatorEvent 给 task
            task.deliverOperatorEvent(operatorId, evt);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalError(t);
            return FutureUtils.completedExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        final Collection<ThreadInfo> threadDump = JvmUtils.createThreadDump();

        final Collection<ThreadDumpInfo.ThreadInfo> threadInfos =
                threadDump.stream()
                        .map(
                                threadInfo ->
                                        ThreadDumpInfo.ThreadInfo.create(
                                                threadInfo.getThreadName(), threadInfo.toString()))
                        .collect(Collectors.toList());

        return CompletableFuture.completedFuture(ThreadDumpInfo.create(threadInfos));
    }

    // ------------------------------------------------------------------------
    //  Internal resource manager connection methods
    // ------------------------------------------------------------------------

    private void notifyOfNewResourceManagerLeader(
            String newLeaderAddress, ResourceManagerId newResourceManagerId) {

        // 获取ResourceManager 地址
        resourceManagerAddress =
                createResourceManagerAddress(newLeaderAddress, newResourceManagerId);

        // 重新连接ResourceManager
        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newLeaderAddress, @Nullable ResourceManagerId newResourceManagerId) {
        if (newLeaderAddress == null) {
            return null;
        } else {
            assert (newResourceManagerId != null);
            return new ResourceManagerAddress(newLeaderAddress, newResourceManagerId);
        }
    }

    private void reconnectToResourceManager(Exception cause) {
        // 关闭之前的连接
        closeResourceManagerConnection(cause);

        // 开始注册, 超时: 默认5分钟
        startRegistrationTimeout();

        // 尝试重新连接ResourceManager
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            // 重新连接
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        // 校验相关
        assert (resourceManagerAddress != null);
        assert (establishedResourceManagerConnection == null);
        assert (resourceManagerConnection == null);

        // Connecting to ResourceManager
        //          akka.tcp://flink@192.168.8.188:62257/user/rpc/resourcemanager_*(00000000000000000000000000000000).
        log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

        // 构建 TaskExecutor Registration
        final TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        getAddress(),
                        getResourceID(),
                        unresolvedTaskManagerLocation.getDataPort(),
                        JMXService.getPort().orElse(-1),
                        hardwareDescription,
                        memoryConfiguration,
                        taskManagerConfiguration.getDefaultSlotResourceProfile(),
                        taskManagerConfiguration.getTotalResourceProfile());

        // 资源管理 Connection
        resourceManagerConnection =
                new TaskExecutorToResourceManagerConnection(
                        log,
                        getRpcService(),
                        taskManagerConfiguration.getRetryingRegistrationConfiguration(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        getMainThreadExecutor(),
                        new ResourceManagerRegistrationListener(),
                        taskExecutorRegistration);


        // 建立连接操作
        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(
            ResourceManagerGateway resourceManagerGateway,
            ResourceID resourceManagerResourceId,
            InstanceID taskExecutorRegistrationId,
            ClusterInformation clusterInformation) {

        // ResourceManager#sendSlotReport
        // 发送 Slot 报告
        final CompletableFuture<Acknowledge> slotReportResponseFuture =
                resourceManagerGateway.sendSlotReport(
                        getResourceID(),
                        taskExecutorRegistrationId,
                        taskSlotTable.createSlotReport(getResourceID()),
                        taskManagerConfiguration.getTimeout());

        slotReportResponseFuture.whenCompleteAsync(
                (acknowledge, throwable) -> {
                    if (throwable != null) {

                        // 重新连接 ResourceManager  抛出异常...
                        reconnectToResourceManager(
                                new TaskManagerException(
                                        "Failed to send initial slot report to ResourceManager.",
                                        throwable));
                    }
                },
                getMainThreadExecutor());

        // 心跳相关
        // monitor the resource manager as heartbeat target
        resourceManagerHeartbeatManager.monitorTarget(
                resourceManagerResourceId,
                new HeartbeatTarget<TaskExecutorHeartbeatPayload>() {
                    @Override
                    public void receiveHeartbeat(
                            ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
                        resourceManagerGateway.heartbeatFromTaskManager(
                                resourceID, heartbeatPayload);
                    }

                    @Override
                    public void requestHeartbeat(
                            ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
                        // the TaskManager won't send heartbeat requests to the ResourceManager
                    }
                });

        // 设置 blob server 地址...
        // set the propagated blob server address
        final InetSocketAddress blobServerAddress =
                new InetSocketAddress(
                        clusterInformation.getBlobServerHostname(),
                        clusterInformation.getBlobServerPort());

        blobCacheService.setBlobServerAddress(blobServerAddress);


        // 建立ResourceManager 连接...
        establishedResourceManagerConnection =
                new EstablishedResourceManagerConnection(
                        resourceManagerGateway,
                        resourceManagerResourceId,
                        taskExecutorRegistrationId);

        // 停止超时操作...
        stopRegistrationTimeout();
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            final ResourceID resourceManagerResourceId =
                    establishedResourceManagerConnection.getResourceManagerResourceId();

            if (log.isDebugEnabled()) {
                log.debug("Close ResourceManager connection {}.", resourceManagerResourceId, cause);
            } else {
                log.info("Close ResourceManager connection {}.", resourceManagerResourceId);
            }
            resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceId);

            ResourceManagerGateway resourceManagerGateway =
                    establishedResourceManagerConnection.getResourceManagerGateway();
            resourceManagerGateway.disconnectTaskManager(getResourceID(), cause);

            establishedResourceManagerConnection = null;

            partitionTracker.stopTrackingAndReleaseAllClusterPartitions();
        }

        if (resourceManagerConnection != null) {
            if (!resourceManagerConnection.isConnected()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress(),
                            cause);
                } else {
                    log.info(
                            "Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress());
                }
            }

            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void startRegistrationTimeout() {
        final Time maxRegistrationDuration = taskManagerConfiguration.getMaxRegistrationDuration();

        if (maxRegistrationDuration != null) {
            final UUID newRegistrationTimeoutId = UUID.randomUUID();
            currentRegistrationTimeoutId = newRegistrationTimeoutId;
            scheduleRunAsync(
                    () -> registrationTimeout(newRegistrationTimeoutId), maxRegistrationDuration);
        }
    }

    private void stopRegistrationTimeout() {
        currentRegistrationTimeoutId = null;
    }

    private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
        if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
            final Time maxRegistrationDuration =
                    taskManagerConfiguration.getMaxRegistrationDuration();

            onFatalError(
                    new RegistrationTimeoutException(
                            String.format(
                                    "Could not register at the ResourceManager within the specified maximum "
                                            + "registration duration %s. This indicates a problem with this instance. Terminating now.",
                                    maxRegistrationDuration)));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal job manager connection methods
    // ------------------------------------------------------------------------

    private void offerSlotsToJobManager(final JobID jobId) {
        // 向JobManager提供Slot : internalOfferSlotsToJobManager
        jobTable.getConnection(jobId).ifPresent(this::internalOfferSlotsToJobManager);
    }

    private void internalOfferSlotsToJobManager(JobTable.Connection jobManagerConnection) {
        // 获取JobID
        final JobID jobId = jobManagerConnection.getJobId();

        // JobID是否已经分配
        if (taskSlotTable.hasAllocatedSlots(jobId)) {

            // Offer reserved slots to the leader of job 694474d11da6100e82744c9e47e2f511.
            log.info("Offer reserved slots to the leader of job {}.", jobId);

            // 获取JobMaster 的  Gateway
            final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

            // 获取 分配给jobId 的所有 TaskSlot
            final Iterator<TaskSlot<Task>> reservedSlotsIterator =  taskSlotTable.getAllocatedSlots(jobId);

            // 获取 JobMasterId
            final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

            // 保留的Slot
            final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

            while (reservedSlotsIterator.hasNext()) {
                SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
                reservedSlots.add(offer);
            }

            // offerSlots
            // Offers the given slots to the job manager.
            // The response contains the set of accepted slots.

            // JobMaster#offerSlots
            CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture =
                    jobMasterGateway.offerSlots(
                            getResourceID(), reservedSlots, taskManagerConfiguration.getTimeout());


            // 异步操作.  处理响应请求,处理异常 || 标记为 slot 状态为active
            acceptedSlotsFuture.whenCompleteAsync(
                    handleAcceptedSlotOffers(jobId, jobMasterGateway, jobMasterId, reservedSlots),
                    getMainThreadExecutor());
        } else {
            log.debug("There are no unassigned slots for the job {}.", jobId);
        }
    }

    @Nonnull
    private BiConsumer<Iterable<SlotOffer>, Throwable> handleAcceptedSlotOffers(
            JobID jobId,
            JobMasterGateway jobMasterGateway,
            JobMasterId jobMasterId,
            Collection<SlotOffer> offeredSlots) {
        return (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
            if (throwable != null) {
                if (throwable instanceof TimeoutException) {

                    // 如果是超时[ TimeoutException ],进行重试操作.
                    log.info(
                            "Slot offering to JobManager did not finish in time. Retrying the slot offering.");
                    // We ran into a timeout. Try again.
                    offerSlotsToJobManager(jobId);
                } else {
                    log.warn(
                            "Slot offering to JobManager failed. Freeing the slots "
                                    + "and returning them to the ResourceManager.",
                            throwable);

                    // We encountered an exception. Free the slots and return them to the RM.
                    for (SlotOffer reservedSlot : offeredSlots) {
                        // 释放slot
                        freeSlotInternal(reservedSlot.getAllocationId(), throwable);
                    }
                }
            } else {
                // check if the response is still valid
                // 检测响应是否有效.
                if (isJobManagerConnectionValid(jobId, jobMasterId)) {
                    // mark accepted slots active
                    // 标记slots状态为active
                    for (SlotOffer acceptedSlot : acceptedSlots) {
                        try {
                            // 激活Slot
                            if (!taskSlotTable.markSlotActive(acceptedSlot.getAllocationId())) {
                                // the slot is either free or releasing at the moment
                                final String message = "Could not mark slot " + jobId + " active.";
                                log.debug(message);
                                jobMasterGateway.failSlot(
                                        getResourceID(),
                                        acceptedSlot.getAllocationId(),
                                        new FlinkException(message));
                            }
                        } catch (SlotNotFoundException e) {
                            final String message = "Could not mark slot " + jobId + " active.";
                            jobMasterGateway.failSlot(
                                    getResourceID(),
                                    acceptedSlot.getAllocationId(),
                                    new FlinkException(message));
                        }

                        offeredSlots.remove(acceptedSlot);
                    }

                    final Exception e = new Exception("The slot was rejected by the JobManager.");

                    for (SlotOffer rejectedSlot : offeredSlots) {
                        freeSlotInternal(rejectedSlot.getAllocationId(), e);
                    }
                } else {
                    // discard the response since there is a new leader for the job
                    log.debug(
                            "Discard offer slot response since there is a new leader "
                                    + "for the job {}.",
                            jobId);
                }
            }
        };
    }

    private void establishJobManagerConnection(
            JobTable.Job job,
            final JobMasterGateway jobMasterGateway,
            JMTMRegistrationSuccess registrationSuccess) {

        final JobID jobId = job.getJobId();
        final Optional<JobTable.Connection> connection = job.asConnection();

        if (connection.isPresent()) {
            JobTable.Connection oldJobManagerConnection = connection.get();

            if (Objects.equals(
                    oldJobManagerConnection.getJobMasterId(), jobMasterGateway.getFencingToken())) {
                // we already are connected to the given job manager
                log.debug(
                        "Ignore JobManager gained leadership message for {} because we are already connected to it.",
                        jobMasterGateway.getFencingToken());
                return;
            } else {
                disconnectJobManagerConnection(
                        oldJobManagerConnection,
                        new Exception("Found new job leader for job id " + jobId + '.'));
            }
        }

        // Establish JobManager connection for job 694474d11da6100e82744c9e47e2f511.
        log.info("Establish JobManager connection for job {}.", jobId);

        ResourceID jobManagerResourceID = registrationSuccess.getResourceID();

        final JobTable.Connection establishedConnection =
                associateWithJobManager(job, jobManagerResourceID, jobMasterGateway);

        // monitor the job manager as heartbeat target
        jobManagerHeartbeatManager.monitorTarget(
                jobManagerResourceID,
                new HeartbeatTarget<TaskExecutorToJobManagerHeartbeatPayload>() {
                    @Override
                    public void receiveHeartbeat(
                            ResourceID resourceID,
                            TaskExecutorToJobManagerHeartbeatPayload payload) {
                        jobMasterGateway.heartbeatFromTaskManager(resourceID, payload);
                    }

                    @Override
                    public void requestHeartbeat(
                            ResourceID resourceID,
                            TaskExecutorToJobManagerHeartbeatPayload payload) {
                        // request heartbeat will never be called on the task manager side
                    }
                });

        internalOfferSlotsToJobManager(establishedConnection);
    }

    private void closeJob(JobTable.Job job, Exception cause) {
        job.asConnection()
                .ifPresent(
                        jobManagerConnection ->
                                disconnectJobManagerConnection(jobManagerConnection, cause));

        job.close();
    }

    private void disconnectJobManagerConnection(
            JobTable.Connection jobManagerConnection, Exception cause) {
        final JobID jobId = jobManagerConnection.getJobId();
        if (log.isDebugEnabled()) {
            log.debug("Close JobManager connection for job {}.", jobId, cause);
        } else {
            log.info("Close JobManager connection for job {}.", jobId);
        }

        // 1. fail tasks running under this JobID
        Iterator<Task> tasks = taskSlotTable.getTasks(jobId);

        final FlinkException failureCause =
                new FlinkException(
                        "JobManager responsible for " + jobId + " lost the leadership.", cause);

        while (tasks.hasNext()) {
            tasks.next().failExternally(failureCause);
        }

        // 2. Move the active slots to state allocated (possible to time out again)
        Set<AllocationID> activeSlotAllocationIDs =
                taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId);

        final FlinkException freeingCause =
                new FlinkException("Slot could not be marked inactive.");

        for (AllocationID activeSlotAllocationID : activeSlotAllocationIDs) {
            try {
                if (!taskSlotTable.markSlotInactive(
                        activeSlotAllocationID, taskManagerConfiguration.getTimeout())) {
                    freeSlotInternal(activeSlotAllocationID, freeingCause);
                }
            } catch (SlotNotFoundException e) {
                log.debug("Could not mark the slot {} inactive.", jobId, e);
            }
        }

        // 3. Disassociate from the JobManager
        try {
            jobManagerHeartbeatManager.unmonitorTarget(jobManagerConnection.getResourceId());
            disassociateFromJobManager(jobManagerConnection, cause);
        } catch (IOException e) {
            log.warn(
                    "Could not properly disassociate from JobManager {}.",
                    jobManagerConnection.getJobManagerGateway().getAddress(),
                    e);
        }

        jobManagerConnection.disconnect();
    }

    private JobTable.Connection associateWithJobManager(
            JobTable.Job job, ResourceID resourceID, JobMasterGateway jobMasterGateway) {
        checkNotNull(resourceID);
        checkNotNull(jobMasterGateway);

        TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobMasterGateway);

        CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);
        GlobalAggregateManager aggregateManager = new RpcGlobalAggregateManager(jobMasterGateway);

        ResultPartitionConsumableNotifier resultPartitionConsumableNotifier =
                new RpcResultPartitionConsumableNotifier(
                        jobMasterGateway,
                        getRpcService().getExecutor(),
                        taskManagerConfiguration.getTimeout());

        PartitionProducerStateChecker partitionStateChecker =
                new RpcPartitionStateChecker(jobMasterGateway);

        registerQueryableState(job.getJobId(), jobMasterGateway);

        return job.connect(
                resourceID,
                jobMasterGateway,
                taskManagerActions,
                checkpointResponder,
                aggregateManager,
                resultPartitionConsumableNotifier,
                partitionStateChecker);
    }

    private void disassociateFromJobManager(
            JobTable.Connection jobManagerConnection, Exception cause) throws IOException {
        checkNotNull(jobManagerConnection);

        final JobID jobId = jobManagerConnection.getJobId();

        // cleanup remaining partitions once all tasks for this job have completed
        scheduleResultPartitionCleanup(jobId);

        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterListener(jobId);
        }

        final KvStateClientProxy kvStateClientProxy = kvStateService.getKvStateClientProxy();

        if (kvStateClientProxy != null) {
            kvStateClientProxy.updateKvStateLocationOracle(jobManagerConnection.getJobId(), null);
        }

        JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
        jobManagerGateway.disconnectTaskManager(getResourceID(), cause);
    }

    private void scheduleResultPartitionCleanup(JobID jobId) {
        final Collection<CompletableFuture<ExecutionState>> taskTerminationFutures =
                taskResultPartitionCleanupFuturesPerJob.remove(jobId);
        if (taskTerminationFutures != null) {
            FutureUtils.waitForAll(taskTerminationFutures)
                    .thenRunAsync(
                            () -> {
                                partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
                            },
                            getMainThreadExecutor());
        }
    }

    private void registerQueryableState(JobID jobId, JobMasterGateway jobMasterGateway) {
        final KvStateServer kvStateServer = kvStateService.getKvStateServer();
        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateServer != null && kvStateRegistry != null) {
            kvStateRegistry.registerListener(
                    jobId,
                    new RpcKvStateRegistryListener(
                            jobMasterGateway, kvStateServer.getServerAddress()));
        }

        final KvStateClientProxy kvStateProxy = kvStateService.getKvStateClientProxy();

        if (kvStateProxy != null) {
            kvStateProxy.updateKvStateLocationOracle(jobId, jobMasterGateway);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal task methods
    // ------------------------------------------------------------------------

    private void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try {
                task.failExternally(cause);
            } catch (Throwable t) {
                log.error("Could not fail task {}.", executionAttemptID, t);
            }
        } else {
            log.debug("Cannot find task to fail for execution {}.", executionAttemptID);
        }
    }

    private void updateTaskExecutionState(
            final JobMasterGateway jobMasterGateway, final TaskExecutionState taskExecutionState) {
        final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

        CompletableFuture<Acknowledge> futureAcknowledge =
                jobMasterGateway.updateTaskExecutionState(taskExecutionState);

        futureAcknowledge.whenCompleteAsync(
                (ack, throwable) -> {
                    if (throwable != null) {
                        failTask(executionAttemptID, throwable);
                    }
                },
                getMainThreadExecutor());
    }

    private void unregisterTaskAndNotifyFinalState(
            final JobMasterGateway jobMasterGateway, final ExecutionAttemptID executionAttemptID) {

        Task task = taskSlotTable.removeTask(executionAttemptID);
        if (task != null) {
            if (!task.getExecutionState().isTerminal()) {
                try {
                    task.failExternally(
                            new IllegalStateException("Task is being remove from TaskManager."));
                } catch (Exception e) {
                    log.error("Could not properly fail task.", e);
                }
            }

            log.info(
                    "Un-registering task and sending final execution state {} to JobManager for task {} {}.",
                    task.getExecutionState(),
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    task.getExecutionId());

            AccumulatorSnapshot accumulatorSnapshot = task.getAccumulatorRegistry().getSnapshot();

            updateTaskExecutionState(
                    jobMasterGateway,
                    new TaskExecutionState(
                            task.getJobID(),
                            task.getExecutionId(),
                            task.getExecutionState(),
                            task.getFailureCause(),
                            accumulatorSnapshot,
                            task.getMetricGroup().getIOMetricGroup().createSnapshot()));
        } else {
            log.error("Cannot find task with ID {} to unregister.", executionAttemptID);
        }
    }

    private void freeSlotInternal(AllocationID allocationId, Throwable cause) {
        checkNotNull(allocationId);

        log.debug("Free slot with allocation id {} because: {}", allocationId, cause.getMessage());

        try {
            final JobID jobId = taskSlotTable.getOwningJob(allocationId);

            // 获取slot 索引的下标.
            final int slotIndex = taskSlotTable.freeSlot(allocationId, cause);

            if (slotIndex != -1) {

                if (isConnectedToResourceManager()) {
                    // 获取ResourceManager
                    // the slot was freed. Tell the RM about it
                    ResourceManagerGateway resourceManagerGateway =
                            establishedResourceManagerConnection.getResourceManagerGateway();
                    // 通知RM slot释放.
                    resourceManagerGateway.notifySlotAvailable(
                            establishedResourceManagerConnection.getTaskExecutorRegistrationId(),
                            new SlotID(getResourceID(), slotIndex),
                            allocationId);
                }

                if (jobId != null) {
                    closeJobManagerConnectionIfNoAllocatedResources(jobId);
                }
            }
        } catch (SlotNotFoundException e) {
            log.debug("Could not free slot for allocation id {}.", allocationId, e);
        }

        // 本地存储清空
        localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
    }

    private void closeJobManagerConnectionIfNoAllocatedResources(JobID jobId) {
        // check whether we still have allocated slots for the same job
        if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty()
                && !partitionTracker.isTrackingPartitionsFor(jobId)) {
            // we can remove the job from the job leader service
            jobLeaderService.removeJob(jobId);

            jobTable.getJob(jobId)
                    .ifPresent(
                            job ->
                                    closeJob(
                                            job,
                                            new FlinkException(
                                                    "TaskExecutor "
                                                            + getAddress()
                                                            + " has no more allocated slots for job "
                                                            + jobId
                                                            + '.')));
        }
    }

    private void timeoutSlot(AllocationID allocationId, UUID ticket) {
        checkNotNull(allocationId);
        checkNotNull(ticket);

        if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
            freeSlotInternal(
                    allocationId, new Exception("The slot " + allocationId + " has timed out."));
        } else {
            log.debug(
                    "Received an invalid timeout for allocation id {} with ticket {}.",
                    allocationId,
                    ticket);
        }
    }

    /**
     * Syncs the TaskExecutor's view on its allocated slots with the JobMaster's view. Slots which
     * are no longer reported by the JobMaster are being freed. Slots which the JobMaster thinks it
     * still owns but which are no longer allocated to it will be failed via {@link
     * JobMasterGateway#failSlot}.
     *
     * @param jobMasterGateway jobMasterGateway to talk to the connected job master
     * @param allocatedSlotReport represents the JobMaster's view on the current slot allocation
     *     state
     */
    private void syncSlotsWithSnapshotFromJobMaster(
            JobMasterGateway jobMasterGateway, AllocatedSlotReport allocatedSlotReport) {
        failNoLongerAllocatedSlots(allocatedSlotReport, jobMasterGateway);
        freeNoLongerUsedSlots(allocatedSlotReport);
    }

    private void failNoLongerAllocatedSlots(
            AllocatedSlotReport allocatedSlotReport, JobMasterGateway jobMasterGateway) {
        for (AllocatedSlotInfo allocatedSlotInfo : allocatedSlotReport.getAllocatedSlotInfos()) {
            final AllocationID allocationId = allocatedSlotInfo.getAllocationId();
            if (!taskSlotTable.isAllocated(
                    allocatedSlotInfo.getSlotIndex(),
                    allocatedSlotReport.getJobId(),
                    allocationId)) {
                jobMasterGateway.failSlot(
                        getResourceID(),
                        allocationId,
                        new FlinkException(
                                String.format(
                                        "Slot %s on TaskExecutor %s is not allocated by job %s.",
                                        allocatedSlotInfo.getSlotIndex(),
                                        getResourceID().getStringWithMetadata(),
                                        allocatedSlotReport.getJobId())));
            }
        }
    }

    private void freeNoLongerUsedSlots(AllocatedSlotReport allocatedSlotReport) {
        final Set<AllocationID> activeSlots =
                taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(allocatedSlotReport.getJobId());
        final Set<AllocationID> reportedSlots =
                allocatedSlotReport.getAllocatedSlotInfos().stream()
                        .map(AllocatedSlotInfo::getAllocationId)
                        .collect(Collectors.toSet());

        final Sets.SetView<AllocationID> difference = Sets.difference(activeSlots, reportedSlots);

        for (AllocationID allocationID : difference) {
            freeSlotInternal(
                    allocationID,
                    new FlinkException(
                            String.format(
                                    "%s is no longer allocated by job %s.",
                                    allocationID, allocatedSlotReport.getJobId())));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal utility methods
    // ------------------------------------------------------------------------

    private boolean isConnectedToResourceManager() {
        return establishedResourceManagerConnection != null;
    }

    private boolean isConnectedToResourceManager(ResourceManagerId resourceManagerId) {
        return establishedResourceManagerConnection != null
                && resourceManagerAddress != null
                && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    private boolean isJobManagerConnectionValid(JobID jobId, JobMasterId jobMasterId) {
        return jobTable.getConnection(jobId)
                .map(jmConnection -> Objects.equals(jmConnection.getJobMasterId(), jobMasterId))
                .orElse(false);
    }

    private CompletableFuture<TransientBlobKey> requestFileUploadByFilePath(
            String filePath, String fileTag) {
        log.debug("Received file upload request for file {}", fileTag);
        if (!StringUtils.isNullOrWhitespaceOnly(filePath)) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        final File file = new File(filePath);
                        if (file.exists()) {
                            try {
                                return putTransientBlobStream(new FileInputStream(file), fileTag)
                                        .get();
                            } catch (Exception e) {
                                log.debug("Could not upload file {}.", fileTag, e);
                                throw new CompletionException(
                                        new FlinkException(
                                                "Could not upload file " + fileTag + '.', e));
                            }
                        } else {
                            log.debug(
                                    "The file {} does not exist on the TaskExecutor {}.",
                                    fileTag,
                                    getResourceID().getStringWithMetadata());
                            throw new CompletionException(
                                    new FlinkException(
                                            "The file "
                                                    + fileTag
                                                    + " does not exist on the TaskExecutor."));
                        }
                    },
                    ioExecutor);
        } else {
            log.debug(
                    "The file {} is unavailable on the TaskExecutor {}.",
                    fileTag,
                    getResourceID().getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new FlinkException(
                            "The file " + fileTag + " is not available on the TaskExecutor."));
        }
    }

    private CompletableFuture<TransientBlobKey> putTransientBlobStream(
            InputStream inputStream, String fileTag) {
        final TransientBlobCache transientBlobService = blobCacheService.getTransientBlobService();
        final TransientBlobKey transientBlobKey;

        try {
            transientBlobKey = transientBlobService.putTransient(inputStream);
        } catch (IOException e) {
            log.debug("Could not upload file {}.", fileTag, e);
            return FutureUtils.completedExceptionally(
                    new FlinkException("Could not upload file " + fileTag + '.', e));
        }
        return CompletableFuture.completedFuture(transientBlobKey);
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public ResourceID getResourceID() {
        return unresolvedTaskManagerLocation.getResourceID();
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    void onFatalError(final Throwable t) {
        try {
            log.error("Fatal error occurred in TaskExecutor {}.", getAddress(), t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    // ------------------------------------------------------------------------
    //  Access to fields for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
        return resourceManagerConnection;
    }

    @VisibleForTesting
    HeartbeatManager<Void, TaskExecutorHeartbeatPayload> getResourceManagerHeartbeatManager() {
        return resourceManagerHeartbeatManager;
    }

    // ------------------------------------------------------------------------
    //  Utility classes
    // ------------------------------------------------------------------------

    /** The listener for leader changes of the resource manager. */
    private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    () ->
                            // 开始执行
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(Exception exception) {
            onFatalError(exception);
        }
    }

    private final class JobLeaderListenerImpl implements JobLeaderListener {

        @Override
        public void jobManagerGainedLeadership(
                final JobID jobId,
                final JobMasterGateway jobManagerGateway,
                final JMTMRegistrationSuccess registrationMessage) {
            runAsync(
                    () ->
                            jobTable.getJob(jobId)
                                    .ifPresent(
                                            job ->
                                                    establishJobManagerConnection(
                                                            job,
                                                            jobManagerGateway,
                                                            registrationMessage)));
        }

        @Override
        public void jobManagerLostLeadership(final JobID jobId, final JobMasterId jobMasterId) {
            log.info(
                    "JobManager for job {} with leader id {} lost leadership.", jobId, jobMasterId);

            runAsync(
                    () ->
                            jobTable.getConnection(jobId)
                                    .ifPresent(
                                            jobManagerConnection ->
                                                    disconnectJobManagerConnection(
                                                            jobManagerConnection,
                                                            new Exception(
                                                                    "Job leader for job id "
                                                                            + jobId
                                                                            + " lost leadership."))));
        }

        @Override
        public void handleError(Throwable throwable) {
            onFatalError(throwable);
        }
    }

    private final class ResourceManagerRegistrationListener
            implements RegistrationConnectionListener<
                    TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> {

        @Override
        public void onRegistrationSuccess(
                TaskExecutorToResourceManagerConnection connection,
                TaskExecutorRegistrationSuccess success) {
            final ResourceID resourceManagerId = success.getResourceManagerId();
            final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
            final ClusterInformation clusterInformation = success.getClusterInformation();
            final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

            // 异步执行
            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality

                        if (resourceManagerConnection == connection) {
                            try {

                                // 建立连接
                                establishResourceManagerConnection(
                                        resourceManagerGateway,
                                        resourceManagerId,
                                        taskExecutorRegistrationId,
                                        clusterInformation);



                            } catch (Throwable t) {
                                log.error(
                                        "Establishing Resource Manager connection in Task Executor failed",
                                        t);
                            }
                        }
                    });
        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            onFatalError(failure);
        }
    }

    private final class TaskManagerActionsImpl implements TaskManagerActions {
        private final JobMasterGateway jobMasterGateway;

        private TaskManagerActionsImpl(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = checkNotNull(jobMasterGateway);
        }

        @Override
        public void notifyFatalError(String message, Throwable cause) {
            try {
                log.error(message, cause);
            } catch (Throwable ignored) {
            }

            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        }

        @Override
        public void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
            runAsync(() -> TaskExecutor.this.failTask(executionAttemptID, cause));
        }

        @Override
        public void updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
            if (taskExecutionState.getExecutionState().isTerminal()) {
                runAsync(
                        () ->
                                unregisterTaskAndNotifyFinalState(
                                        jobMasterGateway, taskExecutionState.getID()));
            } else {
                TaskExecutor.this.updateTaskExecutionState(jobMasterGateway, taskExecutionState);
            }
        }
    }

    private class SlotActionsImpl implements SlotActions {

        @Override
        public void freeSlot(final AllocationID allocationId) {
            runAsync(
                    () ->
                            freeSlotInternal(
                                    allocationId,
                                    new FlinkException(
                                            "TaskSlotTable requested freeing the TaskSlot "
                                                    + allocationId
                                                    + '.')));
        }

        @Override
        public void timeoutSlot(final AllocationID allocationId, final UUID ticket) {
            runAsync(() -> TaskExecutor.this.timeoutSlot(allocationId, ticket));
        }
    }

    private class JobManagerHeartbeatListener
            implements HeartbeatListener<
                    AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            validateRunsInMainThread();
            log.info("The heartbeat of JobManager with id {} timed out.", resourceID);

            jobTable.getConnection(resourceID)
                    .ifPresent(
                            jobManagerConnection ->
                                    disconnectAndTryReconnectToJobManager(
                                            jobManagerConnection,
                                            new TimeoutException(
                                                    "The heartbeat of JobManager with id "
                                                            + resourceID
                                                            + " timed out.")));
        }

        @Override
        public void reportPayload(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            validateRunsInMainThread();
            OptionalConsumer.of(jobTable.getConnection(allocatedSlotReport.getJobId()))
                    .ifPresent(
                            jobManagerConnection -> {
                                syncSlotsWithSnapshotFromJobMaster(
                                        jobManagerConnection.getJobManagerGateway(),
                                        allocatedSlotReport);
                            })
                    .ifNotPresent(
                            () ->
                                    log.debug(
                                            "Ignoring allocated slot report from job {} because there is no active leader.",
                                            allocatedSlotReport.getJobId()));
        }

        @Override
        public TaskExecutorToJobManagerHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return jobTable.getConnection(resourceID)
                    .map(
                            jobManagerConnection -> {
                                JobID jobId = jobManagerConnection.getJobId();

                                Set<ExecutionAttemptID> deployedExecutions = new HashSet<>();
                                List<AccumulatorSnapshot> accumulatorSnapshots =
                                        new ArrayList<>(16);
                                Iterator<Task> allTasks = taskSlotTable.getTasks(jobId);

                                while (allTasks.hasNext()) {
                                    Task task = allTasks.next();
                                    deployedExecutions.add(task.getExecutionId());
                                    accumulatorSnapshots.add(
                                            task.getAccumulatorRegistry().getSnapshot());
                                }
                                return new TaskExecutorToJobManagerHeartbeatPayload(
                                        new AccumulatorReport(accumulatorSnapshots),
                                        new ExecutionDeploymentReport(deployedExecutions));
                            })
                    .orElseGet(TaskExecutorToJobManagerHeartbeatPayload::empty);
        }
    }

    private class ResourceManagerHeartbeatListener
            implements HeartbeatListener<Void, TaskExecutorHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            validateRunsInMainThread();
            // first check whether the timeout is still valid
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceId()
                            .equals(resourceId)) {
                log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

                reconnectToResourceManager(
                        new TaskManagerException(
                                String.format(
                                        "The heartbeat of ResourceManager with id %s timed out.",
                                        resourceId)));
            } else {
                log.debug(
                        "Received heartbeat timeout for outdated ResourceManager id {}. Ignoring the timeout.",
                        resourceId);
            }
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public TaskExecutorHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return new TaskExecutorHeartbeatPayload(
                    taskSlotTable.createSlotReport(getResourceID()),
                    partitionTracker.createClusterPartitionReport());
        }
    }

    @VisibleForTesting
    static final class TaskExecutorJobServices implements JobTable.JobServices {

        private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

        private final Runnable closeHook;

        private TaskExecutorJobServices(
                LibraryCacheManager.ClassLoaderLease classLoaderLease, Runnable closeHook) {
            this.classLoaderLease = classLoaderLease;
            this.closeHook = closeHook;
        }

        @Override
        public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
            return classLoaderLease;
        }

        @Override
        public void close() {
            classLoaderLease.release();
            closeHook.run();
        }

        @VisibleForTesting
        static TaskExecutorJobServices create(
                LibraryCacheManager.ClassLoaderLease classLoaderLease, Runnable closeHook) {
            return new TaskExecutorJobServices(classLoaderLease, closeHook);
        }
    }
}
