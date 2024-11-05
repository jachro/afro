package io.jachro.afro

import cats.syntax.all.*
import io.jachro.afro.Schema.*
import io.jachro.afro.TypeDecoder.Outcome
import io.jachro.afro.all.given
import org.apache.avro.util.Utf8

final private case class RecordTestType(stringValue: String, intValue: Int)

private object RecordTestType:

  val schema: Record[RecordTestType] = Schema
    .Record[RecordTestType](name = "RecordTestType")
    .addField("stringValue", Schema.StringType.typeOnly)
    .addField("intValue", Schema.IntType.typeOnly)

  val avroSchema: String =
    """|{
       |  "type": "record",
       |  "name": "RecordTestType",
       |  "fields": [
       |    {"name": "stringValue", "type": "string"},
       |    {"name": "intValue", "type": "int"}
       |  ]
       |}""".stripMargin

  def avroLibEncoder(tt: RecordTestType): OfficialAvroLibRecord =
    OfficialAvroLibRecord(
      RecordTestType.avroSchema,
      Seq(new Utf8(tt.stringValue), Integer.valueOf(tt.intValue))
    )

  given TypeEncoder[RecordTestType] = TypeEncoder.instance[RecordTestType] { v =>
    List(
      TypeEncoder[String].encodeValue(v.stringValue),
      TypeEncoder[Int].encodeValue(v.intValue)
    ).sequence.map(_.reduce(_ ++ _))
  }

  given TypeDecoder[RecordTestType] = { bv =>
    TypeDecoder[String]
      .decode(bv)
      .flatMap { case Outcome(sv, bv) =>
        TypeDecoder[Int].decode(bv).map { case Outcome(iv, bv) => Outcome((sv, iv), bv) }
      }
      .map { case Outcome(v, bv) => Outcome(RecordTestType.apply.tupled(v), bv) }
  }
