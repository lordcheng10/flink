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
 * 用于标记公共使用的类和方法的注释，但具有不断发展的接口。
 * Annotation to mark classes and methods for public use, but with evolving interfaces.
 *
 * 带有此注释的类和方法用于公共使用，具有稳定的行为。
 * <p>Classes and methods with this annotation are intended for public use and have stable behavior.
 * 但是，它们的接口和签名被认为是不稳定的，可能会在不同版本之间发生变化。
 * However, their interfaces and signatures are not considered to be stable and might be changed across versions.
 *  该注释还排除了在{@link Public}注释的类中具有不断演化的接口/签名的方法和类。
 * <p>This annotation also excludes methods and classes with evolving interfaces / signatures within classes annotated with {@link Public}.
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
@Public
public @interface PublicEvolving {}
