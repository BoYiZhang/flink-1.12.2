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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

/**
 * Class that creates {@link ExecutionVertexSchedulingRequirements} for an {@link ExecutionVertex}.
 */
public final class ExecutionVertexSchedulingRequirementsMapper {

    public static ExecutionVertexSchedulingRequirements from(
            final ExecutionVertex executionVertex) {

        // ********************  executionJobVertex 1  start  ********************
        //    executionVertex = {ExecutionVertex@7788} "Source: Socket Stream (1/1)"
        //    executionVertexId = {ExecutionVertexID@7434} "bc764cd8ddf7a0cff126f51c16239658_0"
        // ********************  executionJobVertex 1   end    ********************

        // ********************  executionJobVertex 2  start  ********************
        //    executionVertex = {ExecutionVertex@7864} "Flat Map (1/4)"
        //    executionVertexId = {ExecutionVertexID@7441} "0a448493b4782967b150582570326227_0"
        //    executionVertex = {ExecutionVertex@7880} "Flat Map (2/4)"
        //    executionVertexId = {ExecutionVertexID@7443} "0a448493b4782967b150582570326227_1"
        //    executionVertex = {ExecutionVertex@7889} "Flat Map (3/4)"
        //    executionVertexId = {ExecutionVertexID@7445} "0a448493b4782967b150582570326227_2"
        //    executionVertex = {ExecutionVertex@7894} "Flat Map (4/4)"
        //    executionVertexId = {ExecutionVertexID@7448} "0a448493b4782967b150582570326227_3"
        // ********************  executionJobVertex 2   end    ********************

        // ********************  executionJobVertex 3  start  ********************
        //    executionVertex = {ExecutionVertex@7725} "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) (1/4)"
        //    executionVertexId = {ExecutionVertexID@7451} "ea632d67b7d595e5b851708ae9ad79d6_0"
        //    executionVertex = {ExecutionVertex@7726} "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) (2/4)"
        //    executionVertexId = {ExecutionVertexID@7453} "ea632d67b7d595e5b851708ae9ad79d6_1"
        //    executionVertex = {ExecutionVertex@7727} "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) (3/4)"
        //    executionVertexId = {ExecutionVertexID@7455} "ea632d67b7d595e5b851708ae9ad79d6_2"
        //    executionVertex = {ExecutionVertex@7728} "Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, ReduceFunction$1, PassThroughWindowFunction) (4/4)"
        //    executionVertexId = {ExecutionVertexID@7457} "ea632d67b7d595e5b851708ae9ad79d6_3"
        // ********************  executionJobVertex 3   end    ********************

        // ********************  executionJobVertex 4  start  ********************
        //    executionVertexId = {ExecutionVertexID@7460} "6d2677a0ecc3fd8df0b72ec675edf8f4_0"
        //    executionVertex = {ExecutionVertex@7914} "Sink: Print to Std. Out (1/1)"
        // ********************  executionJobVertex 4   end    ********************


        final ExecutionVertexID executionVertexId = executionVertex.getID();




        final AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();


        //    slotSharingGroup = {SlotSharingGroup@7717} "SlotSharingGroup [0a448493b4782967b150582570326227, ea632d67b7d595e5b851708ae9ad79d6, 6d2677a0ecc3fd8df0b72ec675edf8f4, bc764cd8ddf7a0cff126f51c16239658]"
        //        ids = {TreeSet@7794}  size = 4
        //               0 = {JobVertexID@7706} "0a448493b4782967b150582570326227"
        //               1 = {JobVertexID@7702} "ea632d67b7d595e5b851708ae9ad79d6"
        //               2 = {JobVertexID@7700} "6d2677a0ecc3fd8df0b72ec675edf8f4"
        //               3 = {JobVertexID@7704} "bc764cd8ddf7a0cff126f51c16239658"
        //        slotSharingGroupId = {SlotSharingGroupId@7795} "786da428f25e64d38481a26d70d39e1c"
        //        resourceSpec = {ResourceSpec@7796} "ResourceSpec{UNKNOWN}"
        final SlotSharingGroup slotSharingGroup =
                executionVertex.getJobVertex().getSlotSharingGroup();


        // 返回 ExecutionVertexSchedulingRequirements 对象

        return new ExecutionVertexSchedulingRequirements.Builder()
                .withExecutionVertexId(executionVertexId)
                .withPreviousAllocationId(latestPriorAllocation)
                .withTaskResourceProfile(executionVertex.getResourceProfile())
                .withPhysicalSlotResourceProfile(getPhysicalSlotResourceProfile(executionVertex))
                .withSlotSharingGroupId(slotSharingGroup.getSlotSharingGroupId())
                .withCoLocationConstraint(executionVertex.getLocationConstraint())
                .build();
    }

    /**
     * Get resource profile of the physical slot to allocate a logical slot in for the given vertex.
     * If the vertex is in a slot sharing group, the physical slot resource profile should be the
     * resource profile of the slot sharing group. Otherwise it should be the resource profile of
     * the vertex itself since the physical slot would be used by this vertex only in this case.
     *
     * @return resource profile of the physical slot to allocate a logical slot for the given vertex
     */
    public static ResourceProfile getPhysicalSlotResourceProfile(
            final ExecutionVertex executionVertex) {
        final SlotSharingGroup slotSharingGroup =
                executionVertex.getJobVertex().getSlotSharingGroup();
        return ResourceProfile.fromResourceSpec(
                slotSharingGroup.getResourceSpec(), MemorySize.ZERO);
    }

    private ExecutionVertexSchedulingRequirementsMapper() {}
}
