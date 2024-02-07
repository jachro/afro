package io.renku.avro4s

import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.given
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Parser as AvroParser
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector

class BinaryEncodingSpec
    extends AnyFlatSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues:

  it should "serialize/deserialize Null value" in:
    val schema: Schema[Null] = Schema.Type.Null(name = "field")
    val b = AvroEncoder.encode[Null](null, schema).value
    AvroDecoder.decode(b, schema).value shouldBe null

  it should "serialize/deserialize Boolean value" in:
    Set(true, false).foreach { v =>
      val schema: Schema[Boolean] = Schema.Type.Boolean(name = "field")

      val actual = AvroEncoder.encode(v, schema).value
      val expected =
        expectedFrom(v, java.lang.Boolean.valueOf, """{"type": "boolean"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder.decode(actual, schema).value shouldBe v
    }

  it should "serialize/deserialize an Int number value" in:
    val schema: Schema[Int] = Schema.Type.Int(name = "field")

    List(0, 1, Byte.MaxValue / 2 + 1, Short.MaxValue / 2 + 1, Int.MaxValue / 2 + 1)
      .flatMap(v => List(-v, v))
      .foreach { v =>
        val actual = AvroEncoder.encode(v, schema).value
        val expected =
          expectedFrom(v, Integer.valueOf, """{"type": "int"}""").toBin
        actual.toBin shouldBe expected

        AvroDecoder.decode(actual, schema).value shouldBe v
      }

  it should "serialize/deserialize a Long number value" in:
    val schema: Schema[Long] = Schema.Type.Long(name = "field")

    List(0, 1, Byte.MaxValue / 2 + 1, Short.MaxValue / 2 + 1, Int.MaxValue / 2 + 1)
      .flatMap(v => List(-v.toLong, v.toLong))
      .foreach { v =>
        val actual = AvroEncoder.encode(v, schema).value
        val expected =
          expectedFrom(v, java.lang.Long.valueOf, """{"type": "long"}""").toBin
        actual.toBin shouldBe expected

        AvroDecoder.decode(actual, schema).value shouldBe v
      }

  it should "serialize/deserialize a Float number value" in:
    val schema: Schema[Float] = Schema.Type.Float(name = "field")

    forAll { (v: Float) =>

      val actual = AvroEncoder.encode(v, schema).value
      val expected =
        expectedFrom(v, java.lang.Float.valueOf, """{"type": "float"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder.decode(actual, schema).value shouldBe v
    }

  it should "serialize/deserialize a Double number value" in:
    val schema: Schema[Double] = Schema.Type.Double(name = "field")

    forAll { (v: Double) =>

      val actual = AvroEncoder.encode(v, schema).value
      val expected =
        expectedFrom(v, java.lang.Double.valueOf, """{"type": "double"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder.decode(actual, schema).value shouldBe v
    }

  it should "serialize/deserialize a sequence of Bytes" in:
    val schema: Schema[ByteVector] = Schema.Type.Bytes(name = "field")

    forAll { (v: Seq[Byte]) =>
      val bvv = ByteVector(v)

      val actual = AvroEncoder.encode(bvv, schema).value
      val expected = expectedFrom(bvv, _.toByteBuffer, """{"type": "bytes"}""").toBin
      actual.toBin shouldBe expected

      AvroDecoder.decode(actual, schema).value shouldBe bvv
    }

  private def expectedFrom[A](
      values: A,
      encoder: A => Any,
      schema: String
  ): ByteVector =
    expectedFromSeq[A](Seq(values), encoder, schema)

  private def expectedFromSeq[A](
      values: Seq[A],
      encoder: A => Any,
      schema: String
  ): ByteVector =
    AvroWriter(parse(schema)).write(values, encoder)

  private lazy val parse: String => AvroSchema =
    new AvroParser().parse
