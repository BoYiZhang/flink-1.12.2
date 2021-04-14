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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 *
 * 定义operator的链接方案。
 * 当一个operator链接到前置operator操作，意味着它们在同一个线程中运行。
 * 一个operator可以包含多个步骤.
 *
 * StreamOperator使用的默认值是{@link#HEAD}，这意味着operator没有链接到它的前一个operator。
 *
 * 大多数的operators 将会以 {@link #ALWAYS} 复写. 意味着他们会尽可能的chained 前置operator。
 *
 *
 *
 * Defines the chaining scheme for the operator.
 *
 * When an operator is chained to the predecessor, it means that they run in the same thread.
 *
 * They become one operator consisting of multiple steps.
 *
 * <p>
 * The default value used by the StreamOperator is {@link #HEAD}, which means that the operator is not chained to its predecessor.
 *
 * Most operators override this with {@link #ALWAYS}, meaning they will be chained to predecessors whenever possible.
 */
@PublicEvolving
public enum ChainingStrategy {

    /**
     *
     * 算子会尽可能的Chain在一起（为了优化性能，最好是使用最大数量的chain和加大算子的并行度）
     *
     *
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal chaining and
     * increase operator parallelism.
     */
    ALWAYS,

    /**
     *
     * 当前算子不会与前置和后置算子进行Chain
     *
     * The operator will not be chained to the preceding or succeeding operators. */
    NEVER,

    /**
     *
     * 当前算子允许被后置算子Chain，但不会与前置算子进行Chain
     * The operator will not be chained to the predecessor, but successors may chain to this operator.
     */
    HEAD,

    /**
     *
     * 与HEAD类似，但此策略会尝试Chain Source算子
     *
     * 该operator将在链的开头运行（类似于{@link #HEAD}，但是如果可能的话，它将另外尝试链接 source 输入。
     * 这使多输入operator可以与多个sources 链接在一起，成为一项任务。
     *
     *
     *
     * This operator will run at the head of a chain (similar as in {@link #HEAD}, but it will additionally try to chain source inputs if possible.
     *
     *
     *
     * This allows multi-input operators to be chained with multiple sources into one task.
     */
    HEAD_WITH_SOURCES;

    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;
}
