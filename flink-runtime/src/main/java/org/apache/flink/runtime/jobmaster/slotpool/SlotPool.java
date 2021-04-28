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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The Interface of a slot pool that manages slots. */
public interface SlotPool extends AllocatedSlotActions, AutoCloseable {

    // ------------------------------------------------------------------------
    //  lifecycle
    // ------------------------------------------------------------------------

    void start(
            JobMasterId jobMasterId,
            String newJobManagerAddress,
            ComponentMainThreadExecutor jmMainThreadScheduledExecutor)
            throws Exception;

    void suspend();

    void close();

    // ------------------------------------------------------------------------
    //  resource manager connection
    // ------------------------------------------------------------------------

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     *
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    void disconnectResourceManager();

    // ------------------------------------------------------------------------
    //  registering / un-registering TaskManagers and slots
    // ------------------------------------------------------------------------

    /**
     * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPool}.
     *
     * @param resourceID identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    boolean registerTaskManager(ResourceID resourceID);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPool}.
     *
     * @param resourceId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    boolean releaseTaskManager(final ResourceID resourceId, final Exception cause);

    /**
     * Offers multiple slots to the {@link SlotPool}. The slot offerings can be individually
     * accepted or rejected by returning the collection of accepted slot offers.
     *
     * @param taskManagerLocation from which the slot offers originate
     * @param taskManagerGateway to talk to the slot offerer
     * @param offers slot offers which are offered to the {@link SlotPool}
     * @return A collection of accepted slot offers. The remaining slot offers are implicitly
     *     rejected.
     */
    Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers);

    /**
     * Fails the slot with the given allocation id.
     *
     * @param allocationID identifying the slot which is being failed
     * @param cause of the failure
     * @return An optional task executor id if this task executor has no more slots registered
     */
    Optional<ResourceID> failAllocation(AllocationID allocationID, Exception cause);

    // ------------------------------------------------------------------------
    //  allocating and disposing slots
    // ------------------------------------------------------------------------

    /**
     * Returns a list of {@link SlotInfoWithUtilization} objects about all slots that are currently
     * available in the slot pool.
     *
     * @return a list of {@link SlotInfoWithUtilization} objects about all slots that are currently
     *     available in the slot pool.
     */
    @Nonnull
    Collection<SlotInfoWithUtilization> getAvailableSlotsInformation();

    /**
     * Returns a list of {@link SlotInfo} objects about all slots that are currently allocated in
     * the slot pool.
     *
     * @return a list of {@link SlotInfo} objects about all slots that are currently allocated in
     *     the slot pool.
     */
    Collection<SlotInfo> getAllocatedSlotsInformation();

    /**
     *
     * 在给定的 request id 下使用给定的 allocation id  分配可用的slot。
     * 如果没有具有给定分配id的slot可用，则此方法返回{@code null}。
     *
     *
     * Allocates the available slot with the given allocation id under the given request id.
     *
     * This method returns {@code null} if no slot with the given allocation id is available.
     *
     * @param slotRequestId identifying the requested slot
     * @param allocationID the allocation id of the requested available slot
     * @return the previously available slot with the given allocation id or {@code null} if no such
     *     slot existed.
     */
    Optional<PhysicalSlot> allocateAvailableSlot(
            @Nonnull SlotRequestId slotRequestId, @Nonnull AllocationID allocationID);

    /**
     * 从resource manager 请求分配新slot。
     *
     * 此方法不会从池中已经可用的slot返回slot，而是将向该池添加一个新slot，该slot将立即分配并返回。
     * 
     * Request the allocation of a new slot from the resource manager.
     *
     * This method will not return a slot from the already available slots from the pool, but instead will add a new slot to that
     * pool that is immediately allocated and returned.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested slot
     * @param timeout timeout for the allocation procedure
     * @return a newly allocated slot that was previously not available.
     */
    @Nonnull
    CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull ResourceProfile resourceProfile,
            @Nullable Time timeout);

    /**
     *
     * 从 resource manager 请求分配新的批处理slot。
     * 与普通slot不同，批处理slot只有在slot池不包含合适的slot时才会超时。
     *
     * 此外，它不会对来自资源管理器的故障信号做出反应。
     *
     * Requests the allocation of a new batch slot from the resource manager.
     *
     * Unlike the normal slot, a batch slot will only time out if the slot pool does not contain a suitable slot.
     * Moreover, it won't react to failure signals from the resource manager.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested batch slot
     * @return a future which is completed with newly allocated batch slot
     */
    @Nonnull
    CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            @Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile);

    /**
     * 
     * 禁用批处理slot请求超时检查。
     * 当其他人要接管超时检查职责时调用。
     * 
     * Disables batch slot request timeout check.
     * Invoked when someone else wants to take over the timeout check responsibility.
     */
    void disableBatchSlotRequestTimeoutCheck();

    /**
     * 创建有关属于指定 task manager 的已分配slot的报告。
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);
}
