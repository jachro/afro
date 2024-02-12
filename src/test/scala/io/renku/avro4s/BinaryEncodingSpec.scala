package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.given
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Parser as AvroParser
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalacheck.Arbitrary
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
    val schema = Schema.Type.NullType(name = "field")

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

  it should "serialize/deserialize a Record value" in:
    case class TestType(stringValue: String, intValue: Int)
    val schema = Schema.Type
      .Record[TestType](name = "TestType")
      .addField("stringValue", Schema.Type.StringType.typeOnly)
      .addField("intValue", Schema.Type.IntType.typeOnly)
    val v = TestType("sv", 1)
    given ValueEncoder[TestType] = ValueEncoder.instance[TestType] { v =>
      List(
        ValueEncoder[String].encodeValue(v.stringValue),
        ValueEncoder[Int].encodeValue(v.intValue)
      ).sequence.map(_.reduce(_ ++ _))
    }
    given ValueDecoder[TestType] = { bv =>
      ValueDecoder[String]
        .decode(bv)
        .flatMap { case (sv, bv) =>
          ValueDecoder[Int].decode(bv).map { case (iv, bv) => (sv, iv) -> bv }
        }
        .map { case (v, bv) => TestType.apply.tupled(v) -> bv }
    }

    val actual = AvroEncoder(schema).encode(v).value
    val avroSchema = """|{
                        |  "type": "record",
                        |  "name": "TestType",
                        |  "fields": [
                        |    {"name": "stringValue", "type": "string"},
                        |    {"name": "intValue", "type": "int"}
                        |  ]
                        |}""".stripMargin
    def record(tt: TestType) =
      AvroRecord(
        avroSchema,
        Seq(new Utf8(tt.stringValue), Integer.valueOf(tt.intValue))
      )
    val expected = expectedFrom(v, record, avroSchema).toBin
    actual.toBin shouldBe expected

    AvroDecoder(schema).decode(actual).value shouldBe v

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

  final case class AvroRecord(schema: String, values: Seq[Any]) extends GenericRecord:

    override def put(key: String, v: Any) = sys.error("immutable record")

    override def put(i: Int, v: Any) = sys.error("Immutable record")

    override def get(key: String): Any = {
      val field = getSchema.getField(key)
      if (field == null)
        sys.error(s"Field $key does not exist in record schema=$schema, values=$values")
      else values(field.pos())
    }

    override def get(i: Int) = values(i)

    override lazy val getSchema = parse(schema)
