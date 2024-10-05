package io.jachro.afro

class UnionSpec extends BinaryEncodingSpec:

  it should "serialize/deserialize an optional value (union of [null, V]) - case of Some" in:

    val v = UnionTestType(Option("a"))

    val actual = AvroEncoder(UnionTestType.schema).encode(v).value
    val expected = expectedFrom(v, UnionTestType.avroLibEncoder, UnionTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(UnionTestType.schema).decode(actual).value shouldBe v

  it should "serialize/deserialize an optional value (union of [null, V]) - case of None" in:

    val v = UnionTestType(Option.empty[String])

    val actual = AvroEncoder(UnionTestType.schema).encode(v).value
    val expected = expectedFrom(v, UnionTestType.avroLibEncoder, UnionTestType.avroSchema)
    actual shouldBe expected

    AvroDecoder(UnionTestType.schema).decode(actual).value shouldBe v
