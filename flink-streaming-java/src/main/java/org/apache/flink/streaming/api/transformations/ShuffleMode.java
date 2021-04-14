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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.PublicEvolving;

/**
 * ShuffleMode 定义了operators 之间交换数据的模式
 * The shuffle mode defines the data exchange mode between operators. */
@PublicEvolving
public enum ShuffleMode {
    /**
     * 生产者和消费者同时在线. 生产出的数据立即会被消费者消费...
     * Producer and consumer are online at the same time. Produced data is received by consumer  immediately.
     */
    PIPELINED,

    /**
     * 生产者先产生数据至完成&停止. 之后 消费者启动消费数据.
     *
     * The producer first produces its entire result and finishes. After that, the consumer is  started and may consume the data.
     */
    BATCH,

    /**
     * shuffle mode : 未定义
     * 由框架决定 shuffle mode.
     * 框架最后将选择{@link ShuffleMode＃BATCH}或{@link ShuffleMode＃PIPELINED}中的一个。
     *
     * The shuffle mode is undefined.
     *
     * It leaves it up to the framework to decide the shuffle mode.
     * The framework will pick one of {@link ShuffleMode#BATCH} or {@link ShuffleMode#PIPELINED} in
     * the end.
     */
    UNDEFINED
}
