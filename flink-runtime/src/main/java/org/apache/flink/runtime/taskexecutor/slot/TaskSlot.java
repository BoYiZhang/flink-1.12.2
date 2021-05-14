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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * 
 * 属于同一slot的多个{@link TaskSlotPayload tasks}的容器。
 * {@link TaskSlot}可以处于以下状态之一：
 * 1. 空闲[Free]-slot为空，未分配给作业
 * 2. 释放中[Releasing]-slot变空后即将释放。
 * 3. 已分配[Allocated]-已为作业分配slot。
 * 4. 活动[Active]-slot正由job manager使用， job manager 是分配job 的负责人
 *
 * 只有在task slot处于空闲状态时才能分配它。
 * 分配的task slot可以转换为活动状态。
 * 
 * 活动slot允许从相应作业添加具有正确分配id的任务。
 * 活动slot可以标记为非活动，从而将状态设置回已分配状态。
 *
 * 分配的或活动的slot只有在为空时才能释放。
 * 如果它不是空的，那么它的状态可以设置为releasing，表示一旦它变空就可以被释放。
 * 
 * 
 * Container for multiple {@link TaskSlotPayload tasks} belonging to the same slot. 
 * A {@link TaskSlot} can be in one of the following states:
 *
 * <ul>
 *   <li>Free - The slot is empty and not allocated to a job
 *   <li>Releasing - The slot is about to be freed after it has become empty.
 *   <li>Allocated - The slot has been allocated for a job.
 *   <li>Active - The slot is in active use by a job manager which is the leader of the allocating
 *       job.
 * </ul>
 *
 * <p>A task slot can only be allocated if it is in state free. An allocated task slot can transit
 * to state active.
 *
 * <p>An active slot allows to add tasks from the respective job and with the correct allocation id.
 * An active slot can be marked as inactive which sets the state back to allocated.
 *
 * <p>An allocated or active slot can only be freed if it is empty. If it is not empty, then it's
 * state can be set to releasing indicating that it can be freed once it becomes empty.
 *
 * @author sysadmin
 * @param <T> type of the {@link TaskSlotPayload} stored in this slot
 */
public class TaskSlot<T extends TaskSlotPayload> implements AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(TaskSlot.class);

    /**
     *  task slot的下标索引
     * Index of the task slot.
     * */
    private final int index;

    /**
     * 此插槽的资源特征。
     * Resource characteristics for this slot.
     * */
    private final ResourceProfile resourceProfile;

    /**
     * 在这个slot中运行的task
     * Tasks running in this slot.
     * */
    private final Map<ExecutionAttemptID, T> tasks;

    /**
     * 内存管理
     */
    private final MemoryManager memoryManager;

    /**
     * slot的状态:
     * 1. ACTIVE : Slot 已经被 job manager 使用
     * 2. ALLOCATED : Slot 已经被分配,但是尚未分配Job manager使用.
     * 3. RELEASING : slot不为空，但任务失败。在移除所有任务后，它将被释放
     *
     * State of this slot. */
    private TaskSlotState state;

    /**
     * 已分配插槽的 job id。
     * Job id to which the slot has been allocated.
     * */
    private final JobID jobId;

    /**
     * 此插槽的Allocation id。
     * Allocation id of this slot.
     * */
    private final AllocationID allocationId;

    /**
     * 当插槽被释放和关闭时，关闭操作完成。
     * The closing future is completed when the slot is freed and closed.
     * */
    private final CompletableFuture<Void> closingFuture;

    /**
     * {@link Executor}用于后台操作，例如验证所有已释放的托管内存
     * {@link Executor} for background actions, e.g. verify all managed memory released.
     * */
    private final Executor asyncExecutor;

    public TaskSlot(
            final int index,
            final ResourceProfile resourceProfile,
            final int memoryPageSize,
            final JobID jobId,
            final AllocationID allocationId,
            final Executor asyncExecutor) {

        this.index = index;


        //    ResourceProfile{
        //        cpuCores=1.0000000000000000,
        //        taskHeapMemory=96.000mb (100663293 bytes),
        //        taskOffHeapMemory=0 bytes,
        //        managedMemory=128.000mb (134217730 bytes),
        //        networkMemory=32.000mb (33554432 bytes)
        //    }
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
        this.asyncExecutor = Preconditions.checkNotNull(asyncExecutor);

        this.tasks = new HashMap<>(4);
        this.state = TaskSlotState.ALLOCATED;

        this.jobId = jobId;
        this.allocationId = allocationId;

        //    resourceProfile = {ResourceProfile@5672} "ResourceProfile{cpuCores=1.0000000000000000, taskHeapMemory=96.000mb (100663293 bytes), taskOffHeapMemory=0 bytes, managedMemory=128.000mb (134217730 bytes), networkMemory=32.000mb (33554432 bytes)}"
        //    memoryPageSize = 32768
        this.memoryManager = createMemoryManager(resourceProfile, memoryPageSize);

        this.closingFuture = new CompletableFuture<>();
    }

    // ----------------------------------------------------------------------------------
    // State accessors
    // ----------------------------------------------------------------------------------

    public int getIndex() {
        return index;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public JobID getJobId() {
        return jobId;
    }

    public AllocationID getAllocationId() {
        return allocationId;
    }

    TaskSlotState getState() {
        return state;
    }

    public boolean isEmpty() {
        return tasks.isEmpty();
    }

    public boolean isActive(JobID activeJobId, AllocationID activeAllocationId) {
        Preconditions.checkNotNull(activeJobId);
        Preconditions.checkNotNull(activeAllocationId);

        return TaskSlotState.ACTIVE == state
                && activeJobId.equals(jobId)
                && activeAllocationId.equals(allocationId);
    }

    public boolean isAllocated(JobID jobIdToCheck, AllocationID allocationIDToCheck) {
        Preconditions.checkNotNull(jobIdToCheck);
        Preconditions.checkNotNull(allocationIDToCheck);

        return jobIdToCheck.equals(jobId)
                && allocationIDToCheck.equals(allocationId)
                && (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state);
    }

    public boolean isReleasing() {
        return TaskSlotState.RELEASING == state;
    }

    /**
     * Get all tasks running in this task slot.
     *
     * @return Iterator to all currently contained tasks in this task slot.
     */
    public Iterator<T> getTasks() {
        return tasks.values().iterator();
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    // ----------------------------------------------------------------------------------
    // State changing methods
    // ----------------------------------------------------------------------------------

    /**
     * 将给定任务添加到任务slot。
     * 仅当还没有另一个具有相同执行尝试ID的任务添加到任务slot时，才有可能。
     * 在这种情况下，该方法返回true。
     * 否则，任务slot将保持不变，并返回false。
     *
     * 如果任务slot状态未激活，则会抛出{@link IllegalStateException}。
     *
     * 如果任务的作业ID和分配ID与为其分配了任务slot的作业ID和分配ID不匹配，则会抛出{@link IllegalArgumentException}。
     *
     * Add the given task to the task slot. This is only possible if there is not already another
     * task with the same execution attempt id added to the task slot. In this case, the method
     * returns true. Otherwise the task slot is left unchanged and false is returned.
     *
     * <p>In case that the task slot state is not active an {@link IllegalStateException} is thrown.
     * In case that the task's job id and allocation id don't match with the job id and allocation
     * id for which the task slot has been allocated, an {@link IllegalArgumentException} is thrown.
     *
     * @param task to be added to the task slot
     * @throws IllegalStateException if the task slot is not in state active
     * @return true if the task was added to the task slot; otherwise false
     */
    public boolean add(T task) {
        // Check that this slot has been assigned to the job sending this task
        Preconditions.checkArgument(
                task.getJobID().equals(jobId),
                "The task's job id does not match the "
                        + "job id for which the slot has been allocated.");
        Preconditions.checkArgument(
                task.getAllocationId().equals(allocationId),
                "The task's allocation "
                        + "id does not match the allocation id for which the slot has been allocated.");
        Preconditions.checkState(
                TaskSlotState.ACTIVE == state, "The task slot is not in state active.");

        T oldTask = tasks.put(task.getExecutionId(), task);

        if (oldTask != null) {
            tasks.put(task.getExecutionId(), oldTask);
            return false;
        } else {
            return true;
        }
    }

    /**
     * 删除由给定的execution  attempt id标识的任务。
     *
     * Remove the task identified by the given execution attempt id.
     *
     * @param executionAttemptId identifying the task to be removed
     * @return The removed task if there was any; otherwise null.
     */
    public T remove(ExecutionAttemptID executionAttemptId) {
        return tasks.remove(executionAttemptId);
    }

    /** Removes all tasks from this task slot. */
    public void clear() {
        tasks.clear();
    }

    /**
     * Mark this slot as active. A slot can only be marked active if it's in state allocated.
     *
     * <p>The method returns true if the slot was set to active. Otherwise it returns false.
     *
     * @return True if the new state of the slot is active; otherwise false
     */
    public boolean markActive() {
        if (TaskSlotState.ALLOCATED == state || TaskSlotState.ACTIVE == state) {
            state = TaskSlotState.ACTIVE;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Mark the slot as inactive/allocated. A slot can only be marked as inactive/allocated if it's
     * in state allocated or active.
     *
     * @return True if the new state of the slot is allocated; otherwise false
     */
    public boolean markInactive() {
        if (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state) {
            state = TaskSlotState.ALLOCATED;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Generate the slot offer from this TaskSlot.
     *
     * @return The sot offer which this task slot can provide
     */
    public SlotOffer generateSlotOffer() {
        Preconditions.checkState(
                TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state,
                "The task slot is not in state active or allocated.");
        Preconditions.checkState(allocationId != null, "The task slot are not allocated");

        return new SlotOffer(allocationId, index, resourceProfile);
    }

    @Override
    public String toString() {
        return "TaskSlot(index:"
                + index
                + ", state:"
                + state
                + ", resource profile: "
                + resourceProfile
                + ", allocationId: "
                + (allocationId != null ? allocationId.toString() : "none")
                + ", jobId: "
                + (jobId != null ? jobId.toString() : "none")
                + ')';
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeAsync(new FlinkException("Closing the slot"));
    }

    /**
     * Close the task slot asynchronously.
     *
     * <p>Slot is moved to {@link TaskSlotState#RELEASING} state and only once. If there are active
     * tasks running in the slot then they are failed. The future of all tasks terminated and slot
     * cleaned up is initiated only once and always returned in case of multiple attempts to close
     * the slot.
     *
     * @param cause cause of closing
     * @return future of all running task if any being done and slot cleaned up.
     */
    CompletableFuture<Void> closeAsync(Throwable cause) {
        if (!isReleasing()) {
            state = TaskSlotState.RELEASING;
            if (!isEmpty()) {
                // we couldn't free the task slot because it still contains task, fail the tasks
                // and set the slot state to releasing so that it gets eventually freed
                tasks.values().forEach(task -> task.failExternally(cause));
            }

            final CompletableFuture<Void> shutdownFuture =
                    FutureUtils.waitForAll(
                                    tasks.values().stream()
                                            .map(TaskSlotPayload::getTerminationFuture)
                                            .collect(Collectors.toList()))
                            .thenRun(memoryManager::shutdown);
            verifyAllManagedMemoryIsReleasedAfter(shutdownFuture);
            FutureUtils.forward(shutdownFuture, closingFuture);
        }
        return closingFuture;
    }

    private void verifyAllManagedMemoryIsReleasedAfter(CompletableFuture<Void> after) {
        after.thenRunAsync(
                () -> {
                    if (!memoryManager.verifyEmpty()) {
                        LOG.warn(
                                "Not all slot managed memory is freed at {}. This usually indicates memory leak. "
                                        + "However, when running an old JVM version it can also be caused by slow garbage collection. "
                                        + "Try to upgrade to Java 8u72 or higher if running on an old Java version.",
                                this);
                    }
                },
                asyncExecutor);
    }

    private static MemoryManager createMemoryManager(
            ResourceProfile resourceProfile, int pageSize) {
        return MemoryManager.create(resourceProfile.getManagedMemory().getBytes(), pageSize);
    }
}
