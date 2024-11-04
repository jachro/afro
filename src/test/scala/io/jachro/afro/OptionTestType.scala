package io.jachro.afro

import io.jachro.afro.all.{*, given}
import org.apache.avro.util.Utf8

final private case class OptionTestType(value: Option[String])

private object OptionTestType:

  val schema: Schema.Record[OptionTestType] = Schema
    .Record[OptionTestType](name = "OptionTestType")
    .addOptionalField("value", Schema.StringType.typeOnly)

  val avroSchema: String =
    """|{
       |  "type": "record",
       |  "name": "OptionTestType",
       |  "fields": [
       |    {"name": "value", "type": ["null", "string"], "default": null}
       |  ]
       |}""".stripMargin

  def avroLibEncoder(tt: OptionTestType): OfficialAvroLibRecord =
    OfficialAvroLibRecord(
      OptionTestType.avroSchema,
      Seq(tt.value.fold(ifEmpty = null)(new Utf8(_)))
    )

  given TypeEncoder[OptionTestType] = TypeEncoder.instance[OptionTestType] {
    case OptionTestType(os) =>
      TypeEncoder[Option[String]].encodeValue(os)
  }

  given TypeDecoder[OptionTestType] =
    TypeDecoder[Option[String]].map(OptionTestType.apply)
