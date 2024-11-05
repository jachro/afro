package io.jachro.afro

import io.jachro.afro.Schema
import org.apache.avro.generic.GenericData
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.*

class CollectionsBinaryEncodingSpec extends BinaryEncodingSpec with Generators:

  it should "serialize/deserialize an Array - case with a positive count" in:

    val schema = Schema.Array(name = "field", Schema.IntType.typeOnly)

    forAll { (l: List[Int]) =>
      val v = l.toArray
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom(
        v,
        _.map(java.lang.Integer.valueOf).toList.asJava,
        """{"type": "array", "items": "int"}"""
      )
      actual shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize a List" in:

    val schema = Schema.Array.forList(name = "field", Schema.IntType.typeOnly)

    forAll { (v: List[Int]) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom(
        v,
        _.map(java.lang.Integer.valueOf).toList.asJava,
        """{"type": "array", "items": "int"}"""
      ).toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize a Set" in:

    val schema: Schema.Array.Iterable[Int, Set] =
      Schema.Array.backedBy[Set, Int](name = "field", Schema.IntType.typeOnly)

    forAll { (v: Set[Int]) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom(
        v,
        _.map(java.lang.Integer.valueOf).toList.asJava,
        """{"type": "array", "items": "int"}"""
      ).toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize an Array - case with a negative count" in:

    val blockSize = minBinaryBlockSize + 1
    val v = Arbitrary.arbBool.generateList(blockSize + 2).toArray

    given TypeEncoder[Array[Boolean]] = blockingArrayEncoder(blockSize)
    val schema = Schema.Array(name = "field", Schema.BooleanType.typeOnly)
    val encoded = AvroEncoder(schema).encode(v).value
    val decodedOfficial = readWithOfficialLib[GenericData.Array[Boolean]](
      encoded,
      """{"type": "array", "items": "boolean"}"""
    )
    decodedOfficial.headOption.value.asScala.toArray[Boolean] shouldBe v

    AvroDecoder(schema).decode(encoded).value shouldBe v

  it should "serialize/deserialize an Array of non-primitive types" in:

    val schema = Schema.Array(name = "field", RecordTestType.schema)

    val v = scala.Array(RecordTestType("tt1", 1), RecordTestType("tt2", 2))
    val actual = AvroEncoder(schema).encode(v).value
    val expected = expectedFrom(
      v,
      _.map(RecordTestType.avroLibEncoder).toList.asJava,
      s"""{"type": "array", "items": ${RecordTestType.avroSchema}}"""
    ).toBin
    actual.toBin shouldBe expected

    AvroDecoder(schema).decode(actual).value shouldBe v
