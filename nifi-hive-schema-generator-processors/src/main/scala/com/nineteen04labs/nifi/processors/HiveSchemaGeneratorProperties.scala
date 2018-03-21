/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nineteen04labs.nifi.processors

// NiFi
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators

trait HiveSchemaGeneratorProperties {
  val TableName =
    new PropertyDescriptor.Builder()
      .name("Table Name")
      .description("Desired Hive table name")
      .required(true)
      .expressionLanguageSupported(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

  val HDFSLocation =
    new PropertyDescriptor.Builder()
      .name("HDFS Location")
      .description("HDFS path where data resides")
      .required(true)
      .expressionLanguageSupported(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

  lazy val properties = List(TableName, HDFSLocation)
}

object HiveSchemaGeneratorProperties extends HiveSchemaGeneratorProperties {
}
