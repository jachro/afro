package io.jachro.afro

import org.apache.avro.generic.{GenericData, GenericFixed}
import org.scalacheck.{Arbitrary, Gen}
import scodec.bits.ByteVector

class FixedTypeSpec extends BinaryEncodingSpec:

  import FixedTestType.{*, given}

  private case class FixedTestType(value: ByteVector):
    lazy val size: Int = value.size.toInt

  private object FixedTestType:

    given TypeEncoder[FixedTestType] =
      fixedTypeEncoder[FixedTestType](_.value)

    def typeDecoder(schema: Schema.FixedType[FixedTestType]): TypeDecoder[FixedTestType] =
      fixedTypeDecoder(FixedTestType.apply, schema)

    lazy val gen: Gen[FixedTestType] =
      for
        size <- Gen.choose(0, 128)
        bytes <- Gen.listOfN(size, Arbitrary.arbByte.arbitrary)
      yield FixedTestType(ByteVector(bytes.toVector))

  it should "serialize/deserialize a value of the fixed type" in:

    forAll(FixedTestType.gen) { v =>

      val schema = Schema.FixedType[FixedTestType](name = "field", size = v.size)

      val actual = AvroEncoder(schema).encode(v).value
      val avroSchema = s"""{"name": "field", "type": "fixed", "size": ${v.size}}"""
      val expected =
        expectedFrom[FixedTestType](
          v,
          v => GenericData.get().createFixed(null, v.value.toArray, parse(avroSchema)),
          avroSchema
        )
      actual shouldBe expected

      given TypeDecoder[FixedTestType] = FixedTestType.typeDecoder(schema)
      AvroDecoder(schema).decode(actual).value shouldBe v
    }
