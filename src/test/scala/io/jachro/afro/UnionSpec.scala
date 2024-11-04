package io.jachro.afro

import cats.syntax.all.*
import scodec.bits.ByteVector

class UnionSpec extends BinaryEncodingSpec:

  it should "serialize/deserialize an optional value (union of [null, V]) - case of Some" in:

    val v = OptionTestType(Option("a"))

    val actual = AvroEncoder(OptionTestType.schema).encode(v).value
    val expected =
      expectedFrom(v, OptionTestType.avroLibEncoder, OptionTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(OptionTestType.schema).decode(actual).value shouldBe v

  it should "serialize/deserialize an optional value (union of [null, V]) - case of None" in:

    val v = OptionTestType(Option.empty[String])

    val actual = AvroEncoder(OptionTestType.schema).encode(v).value
    val expected =
      expectedFrom(v, OptionTestType.avroLibEncoder, OptionTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(OptionTestType.schema).decode(actual).value shouldBe v

  it should "deserialize an optional to default None if no value encoded" in:
    AvroDecoder(OptionTestType.schema)
      .decode(ByteVector.empty)
      .value shouldBe OptionTestType(None)

    TypeDecoder[Option[String]].decode(ByteVector.empty)

  it should "promote the input ByteVector to the Outcome if no value encoded for expected optional" in:

    val in = TypeEncoder[Boolean].encodeValue(true).value

    val o = TypeDecoder[Option[String]].decode(in)

    o.value.value shouldBe None
    o.value.leftBytes shouldBe in

  it should "serialize/deserialize an either value (union of [L, V]) - case of Right" in:

    val v = EitherTestType(1.asRight)

    val actual = AvroEncoder(EitherTestType.schema).encode(v).value
    val expected =
      expectedFrom(v, EitherTestType.avroLibEncoder, EitherTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(EitherTestType.schema).decode(actual).value shouldBe v

  it should "serialize/deserialize an either value (union of [L, V]) - case of Left" in:

    val v = EitherTestType("a".asLeft)

    val actual = AvroEncoder(EitherTestType.schema).encode(v).value
    val expected =
      expectedFrom(v, EitherTestType.avroLibEncoder, EitherTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(EitherTestType.schema).decode(actual).value shouldBe v

  it should "deserialize an either to the default left if no value encoded" in:
    AvroDecoder(EitherTestType.schema)
      .decode(ByteVector.empty)
      .value shouldBe EitherTestType.default

  it should "promote the input ByteVector to the Outcome if no value encoded for expected either" in:

    val in = TypeEncoder[Boolean].encodeValue(true).value
    val default = "def"

    val o = eitherDecoder[String, Int](default).decode(in)

    o.value.value.left.value shouldBe default
    o.value.leftBytes shouldBe in
