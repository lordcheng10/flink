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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * 流的有界性。
 * 一个流要么是有界的（具有有限的record），要么是无限的（具有无限的record）.
 * The boundedness of a stream. A stream could either be "bounded" (a stream with finite records) or
 * "unbounded" (a stream with infinite records).
 * PublicEvolving的作用是啥？
 */
@PublicEvolving
public enum Boundedness {
    /**
     * 一个有界流是具有优先消息书的流。
     * A BOUNDED stream is a stream with finite records.
     * 在源的上下文中，有界流期望源放置它发出的记录的边界。
     * <p>In the context of sources, a BOUNDED stream expects the source to put a boundary of the records it emits.
     * 边界可能是消息数、bytes或者是过去的时间等等。
     * Such boundaries could be number of records, number of bytes, elapsed time, and so on.
     * 如何配置一个流的边界通常是通过源的配置来实现。
     * Such indication of how to bound a stream is typically passed to the sources via configurations.
     * 当源发出一个有界流时，Flink可以利用这个属性在执行过程中进行特定的优化。
     * When the sources emit a BOUNDED stream, Flink may leverage this property to do specific optimizations in the execution.
     *
     *  与无界流不同，有界流通常是对顺序不敏感的。这意味着
     * <p>Unlike unbounded streams, the bounded streams are usually order insensitive.
     * 这意味着源的实现类可以不用追踪事件时间或水印。
     * That means the source implementations may not have to keep track of the event times or watermarks.
     * 取而代之的，是更需要高的吞吐，而不是顺序。
     * Instead, a higher throughput would be preferred.
     */
    BOUNDED,

    /**
     * 一个连续无界流是一个有无限记录的流。
     * A CONTINUOUS_UNBOUNDED stream is a stream with infinite records.
     * 在源的上下文中，一个无限流期望源的实现类是假定不会stop的。
     * <p>In the context of sources, an infinite stream expects the source implementation to run without an upfront indication to Flink that they will eventually stop.
     * 当用户取消或者一些特殊条件遇到后源才可能被中断。
     * The sources may eventually be terminated when users cancel the jobs or some source-specific condition is met.
     * 无界流也可能在某个时刻停止。但是在这之前，flink总是假设源永远运行的。
     * <p>A CONTINUOUS_UNBOUNDED stream may also eventually stop at some point. But before that
     * happens, Flink always assumes the sources are going to run forever.
     */
    CONTINUOUS_UNBOUNDED
}
