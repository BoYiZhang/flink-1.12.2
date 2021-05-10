/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotContext;

/**
 *
 * PhysicalSlot 和 LogicalSlot 这两个概念：
 *      PhysicalSlot 表征的是物理意义上 TaskExecutor 上的一个 slot，
 *      而 LogicalSlot 表征逻辑上的一个 slot，
 *
 *      一个 task 可以部署到一个 LogicalSlot 上，但它和物理上一个具体的 slot 并不是一一对应的。
 *      由于资源共享等机制的存在，多个 LogicalSlot 可能被映射到同一个 PhysicalSlot 上。
 *
 *
 *
 * The context of an {@link AllocatedSlot}. This represent an interface to classes outside the slot
 * pool to interact with allocated slots.
 */
public interface PhysicalSlot extends SlotContext {

    /**
     * Tries to assign the given payload to this allocated slot. This only works if there has not
     * been another payload assigned to this slot.
     *
     * @param payload to assign to this slot
     * @return true if the payload could be assigned, otherwise false
     */
    boolean tryAssignPayload(Payload payload);

    /** Payload which can be assigned to an {@link AllocatedSlot}. */
    interface Payload {

        /**
         * Releases the payload.
         *
         * @param cause of the payload release
         */
        void release(Throwable cause);

        /**
         * Returns whether the payload will occupy a physical slot indefinitely.
         *
         * @return true if the payload will occupy a physical slot indefinitely, otherwise false
         */
        boolean willOccupySlotIndefinitely();
    }
}
