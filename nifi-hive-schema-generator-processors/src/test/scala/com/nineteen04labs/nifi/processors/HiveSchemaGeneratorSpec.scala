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

import java.io._
import scala.io.Source

// ScalaTest
import org.scalatest._

// NiFi
import org.apache.nifi.util.TestRunners

class HiveSchemaGeneratorSpec extends FunSpec {
  import scala.collection.JavaConverters._
  import HiveSchemaGeneratorProperties.{ TableName, HDFSLocation }
  import HiveSchemaGeneratorRelationships.{ RelSuccess, RelFailure }

  val goodContent = Source.fromFile("src/test/resources/flowfile.json").mkString
  val badContent = "ThisIsNotJSON"

  describe("HiveSchemaGenerator") {
    it("should successfully transfer a FlowFile") {
      val processor = new HiveSchemaGenerator
      val runner = TestRunners.newTestRunner(processor)
      runner.setProperty(TableName, "myDataTable")
      runner.setProperty(HDFSLocation, "/test")

      val content = new ByteArrayInputStream(goodContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 1)
      runner.assertTransferCount(RelFailure, 0)

      for (flowFile <- runner.getFlowFilesForRelationship(RelSuccess).asScala) {
        flowFile.assertContentEquals(goodContent)
      }
    }
  }

  describe("HiveSchemaGenerator") {
    it("should fail to transfer a FlowFile") {
      val processor = new HiveSchemaGenerator
      val runner = TestRunners.newTestRunner(processor)
      runner.setProperty(TableName, "myDataTable")
      runner.setProperty(HDFSLocation, "/test")

      val content = new ByteArrayInputStream(badContent.getBytes)
      runner.enqueue(content)
      runner.run(1)

      runner.assertTransferCount(RelSuccess, 0)
      runner.assertTransferCount(RelFailure, 1)

      for (flowFile <- runner.getFlowFilesForRelationship(RelFailure).asScala) {
        flowFile.assertContentEquals(badContent)
      }
    }
  }
}
