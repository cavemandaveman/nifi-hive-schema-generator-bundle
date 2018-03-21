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
import java.util.concurrent.atomic.AtomicReference

// Commons IO
import org.apache.commons.io.IOUtils

// NiFi
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.{ AbstractProcessor, Relationship }
import org.apache.nifi.processor.{ ProcessorInitializationContext, ProcessContext, ProcessSession }
import org.apache.nifi.annotation.behavior.{ ReadsAttribute, ReadsAttributes }
import org.apache.nifi.annotation.behavior.{ WritesAttribute, WritesAttributes }
import org.apache.nifi.annotation.documentation.{ CapabilityDescription, SeeAlso, Tags }
import org.apache.nifi.annotation.lifecycle.OnScheduled

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

        val content = new AtomicReference[String]
        session.read(inFlowFile, new InputStreamCallback {
          override def process(in: InputStream): Unit = {
            try {
              val s = IOUtils.toString(in)
              content.set(s)
            } catch {
              case t: Throwable =>
                getLogger().error(t.getMessage, t)
                session.transfer(inFlowFile, RelFailure)
            }
          }
        })

        class CreateHQL(stream: InputStream) {
          import play.api.libs.json._

          val br = new BufferedReader(new InputStreamReader(stream))

          var line: String = null
          var lines = 0
          var schema: JsValue = Json.obj()
          while ({
            line = br.readLine()
            line != null
          }) {
            lines += 1
            schema = merge(schema, Json.parse(line))
          }
          stream.close()

          case class RowMismatch(a: JsValue, b: JsValue) extends Throwable {
            override def toString = Seq(
              s"On the line $lines you attempted to insert this JSON:",
              Json.prettyPrint(b),
              "with the corresponding schema:",
              out(b),
              "into the schema with this signature:",
              out(a)) mkString "\n"
          }

          case class InconsistentArray(arr: Seq[JsValue]) extends Throwable {
            override def toString =
              "You have an array containing incompatible datatypes:" + Json.prettyPrint(JsArray(arr))
          }

          private def prepare(arr: JsValue) = arr match {
            case x @ JsArray(Seq(_)) => x
            case JsArray(s) =>
              try
                JsArray(Seq(s.foldLeft(JsNull: JsValue)(merge(_, _))))
              catch {
                case RowMismatch(_, _) =>
                  throw new InconsistentArray(s)
              }
            case x => x
          }

          def merge(a: JsValue, b: JsValue): JsValue = {
            (a, prepare(b)) match {
              case (JsNull, x) => x
              case (x, JsNull) => x
              case (x: JsBoolean, _: JsBoolean) => x

              case (a @ JsString(ax), b @ JsString(bx)) =>
                if (ax.size > bx.size) a else b

              case (JsNumber(ax), JsNumber(bx)) => JsNumber((ax max bx) setScale (ax.scale max bx.scale))
              case (JsArray(ax), JsArray(bx)) => JsArray(Seq(merge(ax.head, bx.head)))

              case (JsObject(ax), JsObject(bx)) =>
                JsObject((ax.toSeq ++ bx.toSeq).groupBy(_._1).mapValues {
                  case Seq((_, x)) => prepare(x)
                  case Seq((_, ax), (_, bx)) => merge(ax, bx)
                }.toSeq)

              case _ => throw new RowMismatch(a, b)
            }
          }

          def out(json: JsValue, i: Int = 0, key: Option[String] = None): String = {
            val pad = "\t" * i
            pad + key.fold("")(_ + " ") + (json match {
              case JsNull => "???"
              case _: JsBoolean => "BOOLEAN"

              case JsString(x) if 0 < x.size && x.size < 65356 =>
                s"VARCHAR(${x.size})"
              case _: JsString => "STRING"

              case JsNumber(x) if (x.scale == 0) =>
                if (x.isValidByte) "TINYINT"
                else if (x.isValidShort) "SMALLINT"
                else if (x.isValidInt) "INT"
                else if (x.isValidLong) "BIGINT"
                else s"NUMERIC(${x.precision}, 0)"
              case JsNumber(x) if (x.precision <= 7) => "FLOAT"
              case JsNumber(x) if (x.precision <= 15) => "DOUBLE"
              case JsNumber(x) => s"NUMERIC(${x.precision}, ${x.scale})"

              case JsArray(x) =>
                Seq(
                  "ARRAY<", out(x.head, i + 1), s"$pad>") mkString "\n"

              case JsObject(x) =>
                (Seq("STRUCT<") ++ x.map {
                  case (k, v) =>
                    out(v, i + 1, Some(k + ":"))
                } ++ Seq(s"$pad>")).mkString("\n")
            })
          }

          def definition(i: Int = 0) = schema match {
            case JsObject(x) =>
              x.map {
                case (k, v) => out(v, i, Some(k))
              } mkString ",\n"
            case _ => "ERROR"
          }

          def table(name: String, location: String) = Seq(
            s"CREATE EXTERNAL TABLE $name (",
            definition(1).replace('.', '_'),
            ") ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde';",
            s"location '$location';").mkString("\n")

        }

        val in = session.read(inFlowFile)
        val hql = new CreateHQL(in).table(tableName, location)
        session.putAttribute(inFlowFile, "hiveql-statement", hql)
      }
      case _ =>
        getLogger().warn("FlowFile was null")
    }

    session.transfer(inFlowFile, RelSuccess)
  }
}
