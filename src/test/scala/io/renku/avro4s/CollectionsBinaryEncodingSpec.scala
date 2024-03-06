package io.renku.avro4s

import io.renku.avro4s.Schema.Type
import org.apache.avro.generic.GenericData
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.*

class CollectionsBinaryEncodingSpec extends BinaryEncodingSpec with Generators:

  it should "serialize/deserialize an Array - case with a positive count" in:

    val schema = Schema.Type.Array(name = "field", Schema.Type.IntType.typeOnly)

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

  it should "serialize/deserialize an Array - case with a negative count" in:

    val blockSize = minBinaryBlockSize + 1
    val v = Arbitrary.arbBool.generateList(blockSize + 2).toArray

    given TypeEncoder[Array[Boolean]] = blockingListEncoder(blockSize)
    val schema = Schema.Type.Array(name = "field", Schema.Type.BooleanType.typeOnly)
    val encoded = AvroEncoder(schema).encode(v).value
    val decodedOfficial = readWithOfficialLib[GenericData.Array[Boolean]](
      encoded,
      """{"type": "array", "items": "boolean"}"""
    )
    decodedOfficial.headOption.value.asScala.toArray[Boolean] shouldBe v

    AvroDecoder(schema).decode(encoded).value shouldBe v

  it should "serialize/deserialize an Array of non-primitive types" in:

    val schema = Schema.Type.Array(name = "field", TestType.schema)

    val v = scala.Array(TestType("tt1", 1), TestType("tt2", 2))
    val actual = AvroEncoder(schema).encode(v).value
    val expected = expectedFrom(
      v,
      _.map(TestType.avroLibEncoder).toList.asJava,
      s"""{"type": "array", "items": ${TestType.avroSchema}}"""
    ).toBin
    actual.toBin shouldBe expected

    AvroDecoder(schema).decode(actual).value shouldBe v

  it should "serialize/deserialize a List" in:

    val schema = Schema.Type.Array.forList(name = "field", Schema.Type.IntType.typeOnly)

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

  it should "serialize/deserialize an Iterable" in:

    val schema: Schema.Type.Array.Iterable[Int, Set] =
      Schema.Type.Array.backedBy[Set, Int](name = "field", Schema.Type.IntType.typeOnly)

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
