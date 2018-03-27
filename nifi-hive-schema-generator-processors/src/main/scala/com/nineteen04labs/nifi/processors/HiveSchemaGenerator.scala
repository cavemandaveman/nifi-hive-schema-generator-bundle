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

// Commons IO
import org.apache.commons.io.IOUtils

// NiFi
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.{ AbstractProcessor, Relationship }
import org.apache.nifi.processor.{ ProcessorInitializationContext, ProcessContext, ProcessSession }
import org.apache.nifi.annotation.behavior.{ ReadsAttribute, ReadsAttributes }
import org.apache.nifi.annotation.behavior.{ WritesAttribute, WritesAttributes }
import org.apache.nifi.annotation.documentation.{ CapabilityDescription, SeeAlso, Tags }
import org.apache.nifi.annotation.lifecycle.OnScheduled

import org.json._

@Tags(Array("hive", "database", "sql", "json", "schema"))
@CapabilityDescription("Reads JSON from FlowFile and interprets the schema. An attribute will be created with the generated HiveQL statement")
@SeeAlso(Array())
@ReadsAttributes(Array(
  new ReadsAttribute(attribute = "", description = "")))
@WritesAttributes(Array(
  new WritesAttribute(attribute = "", description = "")))
class HiveSchemaGenerator extends AbstractProcessor with HiveSchemaGeneratorProperties
    with HiveSchemaGeneratorRelationships {

  import scala.collection.JavaConverters._

  protected[this] override def init(context: ProcessorInitializationContext): Unit = {
  }

  override def getSupportedPropertyDescriptors(): java.util.List[PropertyDescriptor] = {
    properties.asJava
  }

  override def getRelationships(): java.util.Set[Relationship] = {
    relationships.asJava
  }

  @OnScheduled
  def onScheduled(context: ProcessContext): Unit = {
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val inFlowFile = session.get

    Option(inFlowFile) match {
      case Some(f) => {
        val tableName =
          context.getProperty(TableName)
            .evaluateAttributeExpressions(inFlowFile)
            .getValue

        val location =
          context.getProperty(HDFSLocation)
            .evaluateAttributeExpressions(inFlowFile)
            .getValue

        val content = session.read(inFlowFile)

        def checkJSONValid(test: String): Boolean = {
          var validity = false
          try {
            new JSONObject(test)
            validity = true
          } catch {
            case ex: JSONException =>
              try {
                new JSONArray(test)
                validity = true
              } catch {
                case ex1: JSONException =>
                  getLogger().error(ex1.getMessage, ex1)
                  validity = false
              }
              getLogger().error(ex.getMessage, ex)
          }
          return validity
        }

        def processOrNot(isValid: Boolean) = {
          val flowFile = session.read(inFlowFile)
          if (isValid) {
            try {
              val hql = new CreateHQL(flowFile).table(tableName, location)
              session.putAttribute(inFlowFile, "hiveql-statement", hql)
              getLogger().debug("Attribute Value: \n" + hql)
              session.transfer(inFlowFile, RelSuccess)
            } catch {
              case t: Throwable =>
                getLogger().error(t.getMessage, t)
                session.transfer(inFlowFile, RelFailure)
            }
          } else {
            session.transfer(inFlowFile, RelFailure)
          }
          flowFile.close()
        }

        val isValidJSON = checkJSONValid(IOUtils.toString(content))
        val _ = processOrNot(isValidJSON)

      }
      case _ =>
        getLogger().warn("FlowFile was null")
    }

  }
}
