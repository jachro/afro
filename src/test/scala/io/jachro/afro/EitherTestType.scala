package io.jachro.afro

import io.jachro.afro.all.{*, given}
import org.apache.avro.util.Utf8

final private case class EitherTestType(value: Either[String, Int])

private object EitherTestType:

  val defaultValue: String = "def"
  val default: EitherTestType = EitherTestType(Left(defaultValue))

  val schema: Schema.Record[EitherTestType] = Schema
    .Record[EitherTestType](name = "EitherTestType")
    .addEitherField(
      "value",
      Schema.StringType.typeOnly,
      Schema.IntType.typeOnly,
      leftDefault = defaultValue
    )

  val avroSchema: String =
    """|{
       |  "type": "record",
       |  "name": "EitherTestType",
       |  "fields": [
       |    {"name": "value", "type": ["string", "int"], "default": "def"}
       |  ]
       |}""".stripMargin

  def avroLibEncoder(tt: EitherTestType): OfficialAvroLibRecord =
    OfficialAvroLibRecord(
      EitherTestType.avroSchema,
      Seq(tt.value.fold(new Utf8(_), identity))
    )

  given TypeEncoder[EitherTestType] = TypeEncoder.instance[EitherTestType] {
    case EitherTestType(either) =>
      TypeEncoder[Either[String, Int]].encodeValue(either)
  }

  given TypeDecoder[EitherTestType] =
    eitherDecoder[String, Int](defaultValue).map(EitherTestType.apply)
