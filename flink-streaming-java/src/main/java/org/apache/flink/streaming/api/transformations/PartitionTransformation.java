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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * 此转换表示输入元素分区的更改。
 * 它不会创建物理操作，它只会影响上游操作与下游操作的连接方式。
 *
 * This transformation represents a change of partitioning of the input elements.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code PartitionTransformation}
 */
@Internal
public class PartitionTransformation<T> extends Transformation<T> {

    // 输入
    private final Transformation<T> input;

    // 分区器
    private final StreamPartitioner<T> partitioner;

    // ShuffleMode : 如果是流的话 应该是: PIPELINED
    private final ShuffleMode shuffleMode;

    /**
     * 根据输入和StreamPartitioner 生成一个PartitionTransformation
     *
     * Creates a new {@code PartitionTransformation} from the given input and {@link StreamPartitioner}.
     *
     * @param input The input {@code Transformation}
     * @param partitioner The {@code StreamPartitioner}
     */
    public PartitionTransformation(Transformation<T> input, StreamPartitioner<T> partitioner) {
        this(input, partitioner, ShuffleMode.UNDEFINED);
    }

    /**
     * Creates a new {@code PartitionTransformation} from the given input and {@link
     * StreamPartitioner}.
     *
     * @param input The input {@code Transformation}
     * @param partitioner The {@code StreamPartitioner}
     * @param shuffleMode The {@code ShuffleMode}
     */
    public PartitionTransformation(
            Transformation<T> input, StreamPartitioner<T> partitioner, ShuffleMode shuffleMode) {
        super("Partition", input.getOutputType(), input.getParallelism());
        this.input = input;
        this.partitioner = partitioner;
        this.shuffleMode = checkNotNull(shuffleMode);
    }

    /**
     * Returns the {@code StreamPartitioner} that must be used for partitioning the elements of the
     * input {@code Transformation}.
     */
    public StreamPartitioner<T> getPartitioner() {
        return partitioner;
    }

    /** Returns the {@link ShuffleMode} of this {@link PartitionTransformation}. */
    public ShuffleMode getShuffleMode() {
        return shuffleMode;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }
}
