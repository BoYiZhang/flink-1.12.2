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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and emits records to the
 * channel selected by the {@link ChannelSelector} for regular {@link #emit(IOReadableWritable)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable>
        extends RecordWriter<T> {

    private final ChannelSelector<T> channelSelector;

    //    writer = {PipelinedResultPartition@6639} "PipelinedResultPartition db1576884668472b75f882792173d0fa#0@eb44184ef213a1ddc71dc739d2f1edee [PIPELINED_BOUNDED, 4 subpartitions, 4 pending consumptions]"
    //            releaseLock = {Object@6643}
    //            consumedSubpartitions = {boolean[4]@6644} [false, false, false, false]
    //            numUnconsumedSubpartitions = 4
    //            subpartitions = {ResultSubpartition[4]@6645}
    //            unicastBufferBuilders = {BufferBuilder[4]@6646}
    //            broadcastBufferBuilder = null
    //            idleTimeMsPerSecond = {MeterView@6647}
    //            owningTaskName = "Flat Map (1/4)#0 (eb44184ef213a1ddc71dc739d2f1edee)"
    //            partitionIndex = 0
    //            partitionId = {ResultPartitionID@6649} "db1576884668472b75f882792173d0fa#0@eb44184ef213a1ddc71dc739d2f1edee"
    //            partitionType = {ResultPartitionType@6650} "PIPELINED_BOUNDED"
    //            partitionManager = {ResultPartitionManager@6651}
    //            numSubpartitions = 4
    //            numTargetKeyGroups = 128
    //            isReleased = {AtomicBoolean@6652} "false"
    //            bufferPool = {LocalBufferPool@6653} "[size: 16, required: 5, requested: 1, available: 1, max: 16, listeners: 0,subpartitions: 4, maxBuffersPerChannel: 10, destroyed: false]"
    //            isFinished = false
    //            cause = null
    //            bufferPoolFactory = {ResultPartitionFactory$lambda@6654}
    //            bufferCompressor = null
    //            numBytesOut = {SimpleCounter@6655}
    //            numBuffersOut = {SimpleCounter@6656}
    //    channelSelector = {KeyGroupStreamPartitioner@6627} "HASH"
    //    timeout = 100
    //    taskName = "Flat Map"
    //    this.channelSelector = {KeyGroupStreamPartitioner@6627} "HASH"
    //    numberOfChannels = 4

    ChannelSelectorRecordWriter(
            ResultPartitionWriter writer,
            ChannelSelector<T> channelSelector,
            long timeout,
            String taskName) {
        super(writer, timeout, taskName);

        this.channelSelector = checkNotNull(channelSelector);
        this.channelSelector.setup(numberOfChannels);
    }

    @Override
    public void emit(T record) throws IOException {

        //    record = {SerializationDelegate@7309}
        //            instance = {StreamRecord@7658} "Record @ (undef) : 正正正"
        //            serializer = {StreamElementSerializer@7319}

        //    channelSelector = {RebalancePartitioner@7311} "REBALANCE"
        //        nextChannelToSendTo = 1
        //        numberOfChannels = 4
        emit(record, channelSelector.selectChannel(record));
    }

    @Override
    public void broadcastEmit(T record) throws IOException {
        checkErroneous();

        // Emitting to all channels in a for loop can be better than calling
        // ResultPartitionWriter#broadcastRecord because the broadcastRecord
        // method incurs extra overhead.
        ByteBuffer serializedRecord = serializeRecord(serializer, record);
        for (int channelIndex = 0; channelIndex < numberOfChannels; channelIndex++) {
            serializedRecord.rewind();
            emit(record, channelIndex);
        }

        if (flushAlways) {
            flushAll();
        }
    }
}
