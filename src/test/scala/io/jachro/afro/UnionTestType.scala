package io.jachro.afro

import io.jachro.afro.all.{*, given}
import org.apache.avro.util.Utf8

final private case class UnionTestType(optionalString: Option[String])

private object UnionTestType:

  val schema: Schema.Record[UnionTestType] = Schema
    .Record[UnionTestType](name = "UnionTestType")
    .addOptionalField("optionalString", Schema.StringType.typeOnly)

  val avroSchema: String =
    """|{
       |  "type": "record",
       |  "name": "UnionTestType",
       |  "fields": [
       |    {"name": "optionalString", "type": ["null", "string"], "default": null}
       |  ]
       |}""".stripMargin

  def avroLibEncoder(tt: UnionTestType): OfficialAvroLibRecord =
    OfficialAvroLibRecord(
      UnionTestType.avroSchema,
      Seq(tt.optionalString.fold(ifEmpty = null)(new Utf8(_)))
    )

  given TypeEncoder[UnionTestType] = TypeEncoder.instance[UnionTestType] {
    case UnionTestType(os) =>
      TypeEncoder[Option[String]].encodeValue(os)
  }

  given TypeDecoder[UnionTestType] =
    TypeDecoder[Option[String]].map(UnionTestType.apply)
