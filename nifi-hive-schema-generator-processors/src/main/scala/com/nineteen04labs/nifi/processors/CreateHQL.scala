package com.nineteen04labs.nifi.processors

import java.io._
import play.api.libs.json._

class CreateHQL(stream: InputStream) {

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
      case JsNull => "STRING"
      case _: JsBoolean => "BOOLEAN"

      /*
      * Use STRING instead of VARCHAR
      * https://community.hortonworks.com/questions/48260/hive-string-vs-varchar-performance.html
      *
      * case JsString(x) if 0 < x.size && x.size < 65356 => s"VARCHAR(${x.size})"
      * case _: JsString => "STRING"
      *
      */
      case JsString(x) => "STRING"

      case JsNumber(x) if (x.scale == 0) =>
        if (x.isValidByte) "INT"
        else if (x.isValidShort) "INT"
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
        "STRUCT<\n" + x.map {
          case (k, v) =>
            out(v, i + 1, Some("`" + k + "`" + ":"))
        }.mkString(",\n") + "\n" + pad + ">"
    })
  }

  def definition(i: Int = 0) = schema match {
    case JsObject(x) =>
      x.map {
        case (k, v) =>
          out(v, i, Some("`" + k + "`"))
      } mkString ",\n"
    case _ => "ERROR"
  }

  def table(name: String, location: String) = Seq(
    s"CREATE EXTERNAL TABLE $name (",
    definition(1).replaceAll("[.-]", "_"),
    ") ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';",
    s"location '$location';").mkString("\n")

}