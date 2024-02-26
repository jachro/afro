package io.renku.avro4s

import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.{*, given}
import org.apache.avro.util.Utf8
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import scodec.bits.ByteVector

class PrimitivesBinaryEncodingSpec extends BinaryEncodingSpec:

  it should "serialize/deserialize Null value" in:
    val schema = Schema.Type.NullType(name = "field")
    given TypeEncoder[Null] = nullTypeEncoder
    given TypeDecoder[Null] = nullTypeDecoder

    val b = AvroEncoder(schema).encode(null).value
    AvroDecoder(schema).decode(b).value shouldBe null

  it should "serialize/deserialize Boolean value" in:
    Set(true, false).foreach { v =>
      val schema = Schema.Type.BooleanType(name = "field")

      val actual = AvroEncoder(schema).encode(v).value
      val expected =
        expectedFrom(v, java.lang.Boolean.valueOf, """{"type": "boolean"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize an Int value" in:
    val schema = Schema.Type.IntType(name = "field")

    List(0, 1, Byte.MaxValue / 2 + 1, Short.MaxValue / 2 + 1, Int.MaxValue / 2 + 1)
      .flatMap(v => List(-v, v))
      .foreach { v =>
        val actual = AvroEncoder(schema).encode(v).value
        val expected =
          expectedFrom(v, Integer.valueOf, """{"type": "int"}""").toBin
        actual.toBin shouldBe expected

        AvroDecoder(schema).decode(actual).value shouldBe v
      }

  it should "serialize/deserialize a Long value" in:
    val schema = Schema.Type.LongType(name = "field")

    List(0, 1, Byte.MaxValue / 2 + 1, Short.MaxValue / 2 + 1, Int.MaxValue / 2 + 1)
      .flatMap(v => List(-v.toLong, v.toLong))
      .foreach { v =>
        val actual = AvroEncoder(schema).encode(v).value
        val expected =
          expectedFrom(v, java.lang.Long.valueOf, """{"type": "long"}""").toBin
        actual.toBin shouldBe expected

        AvroDecoder(schema).decode(actual).value shouldBe v
      }

  it should "serialize/deserialize a Float value" in:
    val schema = Schema.Type.FloatType(name = "field")

    forAll { (v: Float) =>

      val actual = AvroEncoder(schema).encode(v).value
      val expected =
        expectedFrom(v, java.lang.Float.valueOf, """{"type": "float"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize a Double value" in:
    val schema = Schema.Type.DoubleType(name = "field")

    forAll { (v: Double) =>

      val actual = AvroEncoder(schema).encode(v).value
      val expected =
        expectedFrom(v, java.lang.Double.valueOf, """{"type": "double"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize a sequence of Bytes" in:
    val schema = Schema.Type.BytesType(name = "field")

    forAll { (v: Seq[Byte]) =>
      val bvv = ByteVector(v)

      val actual = AvroEncoder(schema).encode(bvv).value
      val expected = expectedFrom(bvv, _.toByteBuffer, """{"type": "bytes"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe bvv
    }

  it should "serialize/deserialize a String value" in:
    val schema = Schema.Type.StringType(name = "field")

    forAll { (v: String) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom(v, new Utf8(_), """{"type": "string"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }
