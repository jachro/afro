package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.given

final private case class TestType(stringValue: String, intValue: Int)

private object TestType:

  val schema: Type.Record[TestType] = Schema.Type
    .Record[TestType](name = "TestType")
    .addField("stringValue", Schema.Type.StringType.typeOnly)
    .addField("intValue", Schema.Type.IntType.typeOnly)

  val avroSchema: String =
    """|{
       |  "type": "record",
       |  "name": "TestType",
       |  "fields": [
       |    {"name": "stringValue", "type": "string"},
       |    {"name": "intValue", "type": "int"}
       |  ]
       |}""".stripMargin

  given TypeEncoder[TestType] = TypeEncoder.instance[TestType] { v =>
    List(
      TypeEncoder[String].encodeValue(v.stringValue),
      TypeEncoder[Int].encodeValue(v.intValue)
    ).sequence.map(_.reduce(_ ++ _))
  }

  given TypeDecoder[TestType] = { bv =>
    TypeDecoder[String]
      .decode(bv)
      .flatMap { case (sv, bv) =>
        TypeDecoder[Int].decode(bv).map { case (iv, bv) => (sv, iv) -> bv }
      }
      .map { case (v, bv) => TestType.apply.tupled(v) -> bv }
  }
