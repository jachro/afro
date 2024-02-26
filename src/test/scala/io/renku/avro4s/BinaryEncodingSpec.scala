package io.renku.avro4s

import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.{*, given}
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Parser as AvroParser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector
import scala.jdk.CollectionConverters.*

class BinaryEncodingSpec
    extends AnyFlatSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues:

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

  it should "serialize/deserialize a Record value" in:

    val v = TestType("sv", 1)

    val actual = AvroEncoder(TestType.schema).encode(v).value
    def record(tt: TestType) =
      AvroRecord(
        TestType.avroSchema,
        Seq(new Utf8(tt.stringValue), Integer.valueOf(tt.intValue))
      )
    val expected = expectedFrom(v, record, TestType.avroSchema).toBin
    actual.toBin shouldBe expected

    AvroDecoder(TestType.schema).decode(actual).value shouldBe v

  it should "serialize/deserialize a nested Record value" in:

    val v = NestedTestType("name", TestType("sv", 1))

    val actual = AvroEncoder(NestedTestType.schema).encode(v).value
    val avroSchema = s"""|{
                         |  "type": "record",
                         |  "name": "NestedTestType",
                         |  "fields": [
                         |    {"name": "name", "type": "string"},
                         |    {"name": "nested", "type": ${TestType.avroSchema}}
                         |  ]
                         |}""".stripMargin
    def record(tt: NestedTestType) =
      AvroRecord(
        avroSchema,
        Seq(
          new Utf8(tt.name),
          AvroRecord(
            TestType.avroSchema,
            Seq(new Utf8(tt.nested.stringValue), Integer.valueOf(tt.nested.intValue))
          )
        )
      )
    val expected = expectedFrom(v, record, avroSchema).toBin
    actual.toBin shouldBe expected

    AvroDecoder(NestedTestType.schema).decode(actual).value shouldBe v

  it should "serialize/deserialize an Enum value" in:
    enum TestEnum:
      case A, B

    val schema = Schema.Type.EnumType(name = "field", TestEnum.values)
    given TypeDecoder[TestEnum] = enumTypeDecoder(using TestEnum)
    val avroSchema = s"""{"type": "enum", "name": "TestEnum", "symbols": ["A", "B"]}"""

    forAll(Gen.oneOf(TestEnum.values.toList)) { (v: TestEnum) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom[TestEnum](
        v,
        v => GenericData.get().createEnum(v.productPrefix, parse(avroSchema)),
        avroSchema
      ).toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize an Array - case with a positive count" in:

    val schema = Schema.Type.Array(name = "field", Schema.Type.IntType.typeOnly)

    forAll { (v: Array[Int]) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom(
        v,
        _.map(i => java.lang.Integer.valueOf(i)).toList.asJava,
        """{"type": "array", "items": "int"}"""
      ).toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
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

  private def parse: String => AvroSchema =
    new AvroParser().parse

  final case class AvroRecord(schema: String, values: Seq[Any]) extends GenericRecord:

    override def put(key: String, v: Any): Unit = sys.error("immutable record")

    override def put(i: Int, v: Any): Unit = sys.error("Immutable record")

    override def get(key: String): Any = {
      val field = getSchema.getField(key)
      if (field == null)
        sys.error(s"Field $key does not exist in record schema=$schema, values=$values")
      else values(field.pos())
    }

    override def get(i: Int): Any = values(i)

    override lazy val getSchema: AvroSchema = parse(schema)
