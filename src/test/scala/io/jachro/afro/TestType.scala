package io.jachro.afro

import cats.syntax.all.*
import io.jachro.afro.Schema.*
import io.jachro.afro.TypeDecoder.Outcome
import io.jachro.afro.all.given
import org.apache.avro.util.Utf8

final private case class TestType(stringValue: String, intValue: Int)

private object TestType:

  val schema: Record[TestType] = Schema
    .Record[TestType](name = "TestType")
    .addField("stringValue", Schema.StringType.typeOnly)
    .addField("intValue", Schema.IntType.typeOnly)

  val avroSchema: String =
    """|{
       |  "type": "record",
       |  "name": "TestType",
       |  "fields": [
       |    {"name": "stringValue", "type": "string"},
       |    {"name": "intValue", "type": "int"}
       |  ]
       |}""".stripMargin

  def avroLibEncoder(tt: TestType): OfficialAvroLibRecord =
    OfficialAvroLibRecord(
      TestType.avroSchema,
      Seq(new Utf8(tt.stringValue), Integer.valueOf(tt.intValue))
    )

  given TypeEncoder[TestType] = TypeEncoder.instance[TestType] { v =>
    List(
      TypeEncoder[String].encodeValue(v.stringValue),
      TypeEncoder[Int].encodeValue(v.intValue)
    ).sequence.map(_.reduce(_ ++ _))
  }

  given TypeDecoder[TestType] = { bv =>
    TypeDecoder[String]
      .decode(bv)
      .flatMap { case Outcome(sv, bv) =>
        TypeDecoder[Int].decode(bv).map { case Outcome(iv, bv) => Outcome((sv, iv), bv) }
      }
      .map { case Outcome(v, bv) => Outcome(TestType.apply.tupled(v), bv) }
  }
