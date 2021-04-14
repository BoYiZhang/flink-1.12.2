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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 *
 * 每个类必须自行选择他们自己的二进制序列&反序列化方式.
 * 特别是，records 必须实现此接口，以便指定如何将其数据传输到二进制表示形式。
 * 实现此接口时，请确保实现类具有默认（无参数）构造函数！
 *
 * This interface must be implemented by every class whose objects have to be serialized to their
 * binary representation and vice-versa.
 *
 * In particular, records have to implement this interface in
 * order to specify how their data can be transferred to a binary representation.
 *
 * <p>When implementing this Interface make sure that the implementing class has a default
 * (zero-argument) constructor!
 */
@Public
public interface IOReadableWritable {

    /**
     * 将对象的内部数据写入给定的数据输出视图。
     *
     * Writes the object's internal data to the given data output view.
     *
     * @param out the output view to receive the data.
     * @throws IOException thrown if any error occurs while writing to the output stream
     */
    void write(DataOutputView out) throws IOException;

    /**
     * 从给定的数据输入视图读取对象的内部数据。
     *
     * Reads the object's internal data from the given data input view.
     *
     * @param in the input view to read the data from
     * @throws IOException thrown if any error occurs while reading from the input stream
     */
    void read(DataInputView in) throws IOException;
}
