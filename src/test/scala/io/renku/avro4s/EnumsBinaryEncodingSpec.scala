package io.renku.avro4s

import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.{*, given}
import org.apache.avro.generic.GenericData
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec

class EnumsBinaryEncodingSpec extends BinaryEncodingSpec:

  private enum TestEnum:
    case A, B

  it should "serialize/deserialize an Enum value" in:

    val schema = Schema.Type.EnumType(name = "field", TestEnum.values)

    given TypeDecoder[TestEnum] = enumTypeDecoder(TestEnum)

    val avroSchema = s"""{"type": "enum", "name": "TestEnum", "symbols": ["A", "B"]}"""

    forAll(Gen.oneOf(TestEnum.values.toList)) { (v: TestEnum) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom[TestEnum](
        v,
        v => GenericData.get().createEnum(v.productPrefix, parse(avroSchema)),
        avroSchema
      )
      actual shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }
