package io.renku.avro4s

import io.renku.avro4s.Schema.Type
import io.renku.avro4s.all.given
import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.*

class CollectionsBinaryEncodingSpec extends BinaryEncodingSpec:

  it should "serialize/deserialize an Array - case with a positive count" in:

    val schema = Schema.Type.Array(name = "field", Schema.Type.IntType.typeOnly)

    forAll { (v: Array[Int]) =>
      val actual = AvroEncoder(schema).encode(v).value
      val expected = expectedFrom(
        v,
        _.map(java.lang.Integer.valueOf).toList.asJava,
        """{"type": "array", "items": "int"}"""
      ).toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }

  it should "serialize/deserialize an Array of non-primitive types" in:

    val schema = Schema.Type.Array(name = "field", TestType.schema)

    val v = Array(TestType("tt1", 1), TestType("tt2", 2))
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
