/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The future default scheduler. */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

    private final Logger log;

    private final ClassLoader userCodeLoader;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionFailureHandler executionFailureHandler;

    private final ScheduledExecutor delayExecutor;

    private final SchedulingStrategy schedulingStrategy;

    private final ExecutionVertexOperations executionVertexOperations;

    private final Set<ExecutionVertexID> verticesWaitingForRestart;

    private final Consumer<ComponentMainThreadExecutor> startUpAction;

    DefaultScheduler(
            final Logger log,
            final JobGraph jobGraph,
            final BackPressureStatsTracker backPressureStatsTracker,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final Consumer<ComponentMainThreadExecutor> startUpAction,
            final ScheduledExecutorService futureExecutor,
            final ScheduledExecutor delayExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final Time rpcTimeout,
            final BlobWriter blobWriter,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final ShuffleMaster<?> shuffleMaster,
            final JobMasterPartitionTracker partitionTracker,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ExecutionVertexOperations executionVertexOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
            final ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp)
            throws Exception {

        super(
                log,
                jobGraph,
                backPressureStatsTracker,
                ioExecutor,
                jobMasterConfiguration,
                new ThrowingSlotProvider(), // this is not used any more in the new scheduler
                futureExecutor,
                userCodeLoader,
                checkpointRecoveryFactory,
                rpcTimeout,
                new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
                blobWriter,
                jobManagerJobMetricGroup,
                Time.seconds(0), // this is not used any more in the new scheduler
                shuffleMaster,
                partitionTracker,
                executionVertexVersioner,
                executionDeploymentTracker,
                false,
                initializationTimestamp);

        this.log = log;

        this.delayExecutor = checkNotNull(delayExecutor);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.executionVertexOperations = checkNotNull(executionVertexOperations);

        final FailoverStrategy failoverStrategy =
                failoverStrategyFactory.create(
                        getSchedulingTopology(), getResultPartitionAvailabilityChecker());
        log.info(
                "Using failover strategy {} for {} ({}).",
                failoverStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());

        this.executionFailureHandler =
                new ExecutionFailureHandler(
                        getSchedulingTopology(), failoverStrategy, restartBackoffTimeStrategy);
        this.schedulingStrategy =
                schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

        this.executionSlotAllocator =
                checkNotNull(executionSlotAllocatorFactory)
                        .createInstance(new DefaultExecutionSlotAllocationContext());

        this.verticesWaitingForRestart = new HashSet<>();
        this.startUpAction = startUpAction;
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    public void setMainThreadExecutor(ComponentMainThreadExecutor mainThreadExecutor) {
        super.setMainThreadExecutor(mainThreadExecutor);
        startUpAction.accept(mainThreadExecutor);
    }

    @Override
    protected long getNumberOfRestarts() {
        return executionFailureHandler.getNumberOfRestarts();
    }

    @Override
    protected void startSchedulingInternal() {
        log.info(
                "Starting scheduling with scheduling strategy [{}]",
                schedulingStrategy.getClass().getName());
        prepareExecutionGraphForNgScheduling();

        // 默认调度策略 : PipelinedRegion   SchedulingStrategy
        // PipelinedRegionSchedulingStrategy#startScheduling
        schedulingStrategy.startScheduling();
    }

    @Override
    protected void updateTaskExecutionStateInternal(
            final ExecutionVertexID executionVertexId,
            final TaskExecutionStateTransition taskExecutionState) {

        schedulingStrategy.onExecutionStateChange(
                executionVertexId, taskExecutionState.getExecutionState());
        maybeHandleTaskFailure(taskExecutionState, executionVertexId);
    }

    private void maybeHandleTaskFailure(
            final TaskExecutionStateTransition taskExecutionState,
            final ExecutionVertexID executionVertexId) {

        if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
            final Throwable error = taskExecutionState.getError(userCodeLoader);
            handleTaskFailure(executionVertexId, error);
        }
    }

    private void handleTaskFailure(
            final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
        setGlobalFailureCause(error);
        notifyCoordinatorsAboutTaskFailure(executionVertexId, error);
        final FailureHandlingResult failureHandlingResult =
                executionFailureHandler.getFailureHandlingResult(executionVertexId, error);
        maybeRestartTasks(failureHandlingResult);
    }

    private void notifyCoordinatorsAboutTaskFailure(
            final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
        final ExecutionJobVertex jobVertex =
                getExecutionJobVertex(executionVertexId.getJobVertexId());
        final int subtaskIndex = executionVertexId.getSubtaskIndex();

        jobVertex.getOperatorCoordinators().forEach(c -> c.subtaskFailed(subtaskIndex, error));
    }

    @Override
    public void handleGlobalFailure(final Throwable error) {
        setGlobalFailureCause(error);

        log.info("Trying to recover from a global failure.", error);
        final FailureHandlingResult failureHandlingResult =
                executionFailureHandler.getGlobalFailureHandlingResult(error);
        maybeRestartTasks(failureHandlingResult);
    }

    private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        if (failureHandlingResult.canRestart()) {
            restartTasksWithDelay(failureHandlingResult);
        } else {
            failJob(failureHandlingResult.getError());
        }
    }

    private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
        final Set<ExecutionVertexID> verticesToRestart =
                failureHandlingResult.getVerticesToRestart();

        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(verticesToRestart)
                                .values());
        final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

        addVerticesToRestartPending(verticesToRestart);

        final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

        delayExecutor.schedule(
                () ->
                        FutureUtils.assertNoException(
                                cancelFuture.thenRunAsync(
                                        restartTasks(executionVertexVersions, globalRecovery),
                                        getMainThreadExecutor())),
                failureHandlingResult.getRestartDelayMS(),
                TimeUnit.MILLISECONDS);
    }

    private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.addAll(verticesToRestart);
        transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
    }

    private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.removeAll(verticesToRestart);
        if (verticesWaitingForRestart.isEmpty()) {
            transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
        }
    }

    private Runnable restartTasks(
            final Set<ExecutionVertexVersion> executionVertexVersions,
            final boolean isGlobalRecovery) {
        return () -> {
            final Set<ExecutionVertexID> verticesToRestart =
                    executionVertexVersioner.getUnmodifiedExecutionVertices(
                            executionVertexVersions);

            removeVerticesFromRestartPending(verticesToRestart);

            resetForNewExecutions(verticesToRestart);

            try {
                restoreState(verticesToRestart, isGlobalRecovery);
            } catch (Throwable t) {
                handleGlobalFailure(t);
                return;
            }

            schedulingStrategy.restartTasks(verticesToRestart);
        };
    }

    private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
        final List<CompletableFuture<?>> cancelFutures =
                verticesToRestart.stream()
                        .map(this::cancelExecutionVertex)
                        .collect(Collectors.toList());

        return FutureUtils.combineAll(cancelFutures);
    }

    private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
        final ExecutionVertex vertex = getExecutionVertex(executionVertexId);

        notifyCoordinatorOfCancellation(vertex);

        executionSlotAllocator.cancel(executionVertexId);
        return executionVertexOperations.cancel(vertex);
    }

    @Override
    protected void scheduleOrUpdateConsumersInternal(
            final IntermediateResultPartitionID partitionId) {
        schedulingStrategy.onPartitionConsumable(partitionId);
    }

    // ------------------------------------------------------------------------
    // SchedulerOperations
    // ------------------------------------------------------------------------

    @Override
    public void allocateSlotsAndDeploy(
            final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
        validateDeploymentOptions(executionVertexDeploymentOptions);


        //    {ExecutionVertexID@7434} "bc764cd8ddf7a0cff126f51c16239658_0" -> {ExecutionVertexDeploymentOption@7484}
        //    {ExecutionVertexID@7453} "ea632d67b7d595e5b851708ae9ad79d6_1" -> {ExecutionVertexDeploymentOption@7485}
        //    {ExecutionVertexID@7445} "0a448493b4782967b150582570326227_2" -> {ExecutionVertexDeploymentOption@7486}
        //    {ExecutionVertexID@7451} "ea632d67b7d595e5b851708ae9ad79d6_0" -> {ExecutionVertexDeploymentOption@7487}
        //    {ExecutionVertexID@7448} "0a448493b4782967b150582570326227_3" -> {ExecutionVertexDeploymentOption@7488}
        //    {ExecutionVertexID@7460} "6d2677a0ecc3fd8df0b72ec675edf8f4_0" -> {ExecutionVertexDeploymentOption@7489}
        //    {ExecutionVertexID@7457} "ea632d67b7d595e5b851708ae9ad79d6_3" -> {ExecutionVertexDeploymentOption@7490}
        //    {ExecutionVertexID@7441} "0a448493b4782967b150582570326227_0" -> {ExecutionVertexDeploymentOption@7491}
        //    {ExecutionVertexID@7455} "ea632d67b7d595e5b851708ae9ad79d6_2" -> {ExecutionVertexDeploymentOption@7492}
        //    {ExecutionVertexID@7443} "0a448493b4782967b150582570326227_1" -> {ExecutionVertexDeploymentOption@7493}
        final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
                groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

        //    0 = {ExecutionVertexID@7434} "bc764cd8ddf7a0cff126f51c16239658_0"
        //    1 = {ExecutionVertexID@7441} "0a448493b4782967b150582570326227_0"
        //    2 = {ExecutionVertexID@7443} "0a448493b4782967b150582570326227_1"
        //    3 = {ExecutionVertexID@7445} "0a448493b4782967b150582570326227_2"
        //    4 = {ExecutionVertexID@7448} "0a448493b4782967b150582570326227_3"
        //    5 = {ExecutionVertexID@7451} "ea632d67b7d595e5b851708ae9ad79d6_0"
        //    6 = {ExecutionVertexID@7453} "ea632d67b7d595e5b851708ae9ad79d6_1"
        //    7 = {ExecutionVertexID@7455} "ea632d67b7d595e5b851708ae9ad79d6_2"
        //    8 = {ExecutionVertexID@7457} "ea632d67b7d595e5b851708ae9ad79d6_3"
        //    9 = {ExecutionVertexID@7460} "6d2677a0ecc3fd8df0b72ec675edf8f4_0"
        final List<ExecutionVertexID> verticesToDeploy =
                executionVertexDeploymentOptions.stream()
                        .map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                        .collect(Collectors.toList());


        //    {ExecutionVertexID@7434} "bc764cd8ddf7a0cff126f51c16239658_0" -> {ExecutionVertexVersion@7566}
        //          key = {ExecutionVertexID@7434} "bc764cd8ddf7a0cff126f51c16239658_0"
        //          value = {ExecutionVertexVersion@7566}
        //              executionVertexId = {ExecutionVertexID@7434} "bc764cd8ddf7a0cff126f51c16239658_0"
        //              version = 1
        //    {ExecutionVertexID@7453} "ea632d67b7d595e5b851708ae9ad79d6_1" -> {ExecutionVertexVersion@7567}
        //          key = {ExecutionVertexID@7453} "ea632d67b7d595e5b851708ae9ad79d6_1"
        //          value = {ExecutionVertexVersion@7567}
        //                executionVertexId = {ExecutionVertexID@7453} "ea632d67b7d595e5b851708ae9ad79d6_1"
        //                version = 1
        //    {ExecutionVertexID@7445} "0a448493b4782967b150582570326227_2" -> {ExecutionVertexVersion@7568}
        //    {ExecutionVertexID@7451} "ea632d67b7d595e5b851708ae9ad79d6_0" -> {ExecutionVertexVersion@7569}
        //    {ExecutionVertexID@7448} "0a448493b4782967b150582570326227_3" -> {ExecutionVertexVersion@7570}
        //    {ExecutionVertexID@7460} "6d2677a0ecc3fd8df0b72ec675edf8f4_0" -> {ExecutionVertexVersion@7571}
        //    {ExecutionVertexID@7457} "ea632d67b7d595e5b851708ae9ad79d6_3" -> {ExecutionVertexVersion@7572}
        //    {ExecutionVertexID@7441} "0a448493b4782967b150582570326227_0" -> {ExecutionVertexVersion@7573}
        //    {ExecutionVertexID@7455} "ea632d67b7d595e5b851708ae9ad79d6_2" -> {ExecutionVertexVersion@7574}
        //    {ExecutionVertexID@7443} "0a448493b4782967b150582570326227_1" -> {ExecutionVertexVersion@7575}
        final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
                executionVertexVersioner.recordVertexModifications(verticesToDeploy);

        // 修改ExecutionVertex 状态为SCHEDULED
        transitionToScheduled(verticesToDeploy);

        // allocateSlots ??
        //    slotExecutionVertexAssignments = {ArrayList@8148}  size = 10
        //        0 = {SlotExecutionVertexAssignment@8064}
        //        1 = {SlotExecutionVertexAssignment@8070}
        //        2 = {SlotExecutionVertexAssignment@8072}
        //        3 = {SlotExecutionVertexAssignment@8066}
        //        4 = {SlotExecutionVertexAssignment@8068}
        //        5 = {SlotExecutionVertexAssignment@8067}
        //        6 = {SlotExecutionVertexAssignment@8065}
        //        7 = {SlotExecutionVertexAssignment@8073}
        //        8 = {SlotExecutionVertexAssignment@8071}
        //        9 = {SlotExecutionVertexAssignment@8069}
        final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
                allocateSlots(executionVertexDeploymentOptions);

        // DeploymentHandle包含 : 版本, 参数 , slot分配信息
        //    deploymentHandles = {ArrayList@8194}  size = 10
        //        0 = {DeploymentHandle@8212}
        //            requiredVertexVersion = {ExecutionVertexVersion@7566}
        //            executionVertexDeploymentOption = {ExecutionVertexDeploymentOption@7484}
        //            slotExecutionVertexAssignment = {SlotExecutionVertexAssignment@8064}
        //        1 = {DeploymentHandle@8213}
        //            requiredVertexVersion = {ExecutionVertexVersion@7573}
        //            executionVertexDeploymentOption = {ExecutionVertexDeploymentOption@7491}
        //            slotExecutionVertexAssignment = {SlotExecutionVertexAssignment@8070}
        //        2 = {DeploymentHandle@8214}
        //        3 = {DeploymentHandle@8215}
        //        4 = {DeploymentHandle@8216}
        //        5 = {DeploymentHandle@8217}
        //        6 = {DeploymentHandle@8218}
        //        7 = {DeploymentHandle@8219}
        //        8 = {DeploymentHandle@8220}
        //        9 = {DeploymentHandle@8221}
        final List<DeploymentHandle> deploymentHandles =
                createDeploymentHandles(
                        requiredVersionByVertex,
                        deploymentOptionsByVertex,
                        slotExecutionVertexAssignments);

        // 开始部署 ...
        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateDeploymentOptions(
            final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
        deploymentOptions.stream()
                .map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                .map(this::getExecutionVertex)
                .forEach(
                        v ->
                                checkState(
                                        v.getExecutionState() == ExecutionState.CREATED,
                                        "expected vertex %s to be in CREATED state, was: %s",
                                        v.getID(),
                                        v.getExecutionState()));
    }

    private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption>
            groupDeploymentOptionsByVertexId(
                    final Collection<ExecutionVertexDeploymentOption>
                            executionVertexDeploymentOptions) {
        return executionVertexDeploymentOptions.stream()
                .collect(
                        Collectors.toMap(
                                ExecutionVertexDeploymentOption::getExecutionVertexId,
                                Function.identity()));
    }

    private List<SlotExecutionVertexAssignment> allocateSlots(
            final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
        // 分配slots ?
        return executionSlotAllocator.allocateSlotsFor(
                executionVertexDeploymentOptions.stream()
                        .map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                        .map(this::getExecutionVertex)
                        // 分配...
                        .map(ExecutionVertexSchedulingRequirementsMapper::from)
                        .collect(Collectors.toList()));
    }

    private static List<DeploymentHandle> createDeploymentHandles(
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
            final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

        return slotExecutionVertexAssignments.stream()
                .map(
                        slotExecutionVertexAssignment -> {
                            final ExecutionVertexID executionVertexId =
                                    slotExecutionVertexAssignment.getExecutionVertexId();
                            return new DeploymentHandle(
                                    requiredVersionByVertex.get(executionVertexId),
                                    deploymentOptionsByVertex.get(executionVertexId),
                                    slotExecutionVertexAssignment);
                        })
                .collect(Collectors.toList());
    }

    private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
        //  分配资源, 开始部署
        FutureUtils.assertNoException(
                assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
    }

    private CompletableFuture<Void> assignAllResources(
            final List<DeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
        for (DeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> slotAssigned =
                    deploymentHandle
                            .getSlotExecutionVertexAssignment()
                            .getLogicalSlotFuture()
                            .handle(assignResourceOrHandleError(deploymentHandle));
            slotAssignedFutures.add(slotAssigned);
        }
        return FutureUtils.waitForAll(slotAssignedFutures);
    }

    private BiFunction<Void, Throwable, Void> deployAll(
            final List<DeploymentHandle> deploymentHandles) {


        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);
            for (final DeploymentHandle deploymentHandle : deploymentHandles) {
                final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
                        deploymentHandle.getSlotExecutionVertexAssignment();
                final CompletableFuture<LogicalSlot> slotAssigned =
                        slotExecutionVertexAssignment.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());
                // slot 分配任务 : deployOrHandleError
                FutureUtils.assertNoException(
                        slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(
            final DeploymentHandle deploymentHandle) {
        final ExecutionVertexVersion requiredVertexVersion =
                deploymentHandle.getRequiredVertexVersion();
        final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

        return (logicalSlot, throwable) -> {
            if (executionVertexVersioner.isModified(requiredVertexVersion)) {
                log.debug(
                        "Refusing to assign slot to execution vertex {} because this deployment was "
                                + "superseded by another deployment",
                        executionVertexId);
                releaseSlotIfPresent(logicalSlot);
                return null;
            }

            if (throwable == null) {
                final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
                final boolean sendScheduleOrUpdateConsumerMessage =
                        deploymentHandle
                                .getDeploymentOption()
                                .sendScheduleOrUpdateConsumerMessage();
                executionVertex
                        .getCurrentExecutionAttempt()
                        .registerProducedPartitions(
                                logicalSlot.getTaskManagerLocation(),
                                sendScheduleOrUpdateConsumerMessage);
                executionVertex.tryAssignResource(logicalSlot);
            } else {
                handleTaskDeploymentFailure(
                        executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
            }
            return null;
        };
    }

    private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private void handleTaskDeploymentFailure(
            final ExecutionVertexID executionVertexId, final Throwable error) {
        executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. "
                            + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    private BiFunction<Object, Throwable, Void> deployOrHandleError(
            final DeploymentHandle deploymentHandle) {
        final ExecutionVertexVersion requiredVertexVersion =
                deploymentHandle.getRequiredVertexVersion();
        final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

        return (ignored, throwable) -> {
            if (executionVertexVersioner.isModified(requiredVertexVersion)) {
                log.debug(
                        "Refusing to deploy execution vertex {} because this deployment was "
                                + "superseded by another deployment",
                        executionVertexId);
                return null;
            }

            if (throwable == null) {
                // 部署task
                deployTaskSafe(executionVertexId);
            } else {
                handleTaskDeploymentFailure(executionVertexId, throwable);
            }
            return null;
        };
    }

    private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
        try {
            // 获取ExecutionVertex
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
            // 开始部署 : ExecutionVertex
            // DefaultExecutionVertexOperations#deploy
            executionVertexOperations.deploy(executionVertex);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(executionVertexId, e);
        }
    }

    private void notifyCoordinatorOfCancellation(ExecutionVertex vertex) {
        // this method makes a best effort to filter out duplicate notifications, meaning cases
        // where
        // the coordinator was already notified for that specific task
        // we don't notify if the task is already FAILED, CANCELLING, or CANCELED

        final ExecutionState currentState = vertex.getExecutionState();
        if (currentState == ExecutionState.FAILED
                || currentState == ExecutionState.CANCELING
                || currentState == ExecutionState.CANCELED) {
            return;
        }

        for (OperatorCoordinator coordinator : vertex.getJobVertex().getOperatorCoordinators()) {
            coordinator.subtaskFailed(vertex.getParallelSubtaskIndex(), null);
        }
    }

    private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

        @Override
        public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getResourceProfile();
        }

        @Override
        public AllocationID getPriorAllocationId(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getLatestPriorAllocation();
        }

        @Override
        public SchedulingTopology getSchedulingTopology() {
            return DefaultScheduler.this.getSchedulingTopology();
        }

        @Override
        public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
            return getJobGraph().getSlotSharingGroups();
        }

        @Override
        public Set<CoLocationGroupDesc> getCoLocationGroups() {
            return getJobGraph().getCoLocationGroupDescriptors();
        }

        @Override
        public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertexId);
        }

        @Override
        public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
        }

        @Override
        public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
            return stateLocationRetriever.getStateLocation(executionVertexId);
        }
    }
}
