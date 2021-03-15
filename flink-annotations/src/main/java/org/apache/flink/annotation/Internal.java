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
 *
 */

package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 *
 * 通过里面的注释可以知道这个注解含义：
 * 将稳定的公共API中的方法标记为内部开发人员API的注释。开发人员api是稳定的，但在Flink内部，
 * 可能会在不同版本之间发生变化。个人理解：所以被这个注解标记的api接口外部人员最好不要用，因为容易随着版本变化发生改变，
 * 从而影响到外部人员使用。
 *
 * Annotation to mark methods within stable, public APIs as an internal developer API.
 *
 * <p>Developer APIs are stable but internal to Flink and might change across releases.
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR })
@Public
public @interface Internal {
}
