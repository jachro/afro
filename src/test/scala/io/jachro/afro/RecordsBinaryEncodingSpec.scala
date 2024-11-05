package io.jachro.afro

import org.apache.avro.util.Utf8

class RecordsBinaryEncodingSpec extends BinaryEncodingSpec:

  it should "serialize/deserialize a Record value" in:

    val v = RecordTestType("sv", 1)

    val actual = AvroEncoder(RecordTestType.schema).encode(v).value
    val expected = expectedFrom(v, RecordTestType.avroLibEncoder, RecordTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(RecordTestType.schema).decode(actual).value shouldBe v

  it should "serialize/deserialize a nested Record value" in:

    val v = NestedTestType("name", RecordTestType("sv", 1))

    val actual = AvroEncoder(NestedTestType.schema).encode(v).value
    val avroSchema = s"""|{
                         |  "type": "record",
                         |  "name": "NestedTestType",
                         |  "fields": [
                         |    {"name": "name", "type": "string"},
                         |    {"name": "nested", "type": ${RecordTestType.avroSchema}}
                         |  ]
                         |}""".stripMargin
    def record(tt: NestedTestType) =
      OfficialAvroLibRecord(
        avroSchema,
        Seq(
          new Utf8(tt.name),
          OfficialAvroLibRecord(
            RecordTestType.avroSchema,
            Seq(new Utf8(tt.nested.stringValue), Integer.valueOf(tt.nested.intValue))
          )
        )
      )
    val expected = expectedFrom(v, record, avroSchema)
    actual shouldBe expected

    AvroDecoder(NestedTestType.schema).decode(actual).value shouldBe v
