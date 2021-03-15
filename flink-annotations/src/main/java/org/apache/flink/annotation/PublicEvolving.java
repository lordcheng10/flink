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
 * 标注类和方法以供公共使用，但具有不断发展的接口。带有此注释的类和方法用于公共使用，并且具有稳定的行为。
 * 然而，它们的接口和签名不被认为是稳定的，可能会被更改由于版本更新。
 *
 * 关于flink的注解参考：https://blog.csdn.net/qq1010234991/article/details/104073553
 *
 * Annotation to mark classes and methods for public use, but with evolving interfaces.
 *
 * <p>Classes and methods with this annotation are intended for public use and have stable behavior.
 * However, their interfaces and signatures are not considered to be stable and might be changed
 * across versions.
 *
 * <p>This annotation also excludes methods and classes with evolving interfaces / signatures
 * within classes annotated with {@link Public}.
 *
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR })
@Public
public @interface PublicEvolving {
}
