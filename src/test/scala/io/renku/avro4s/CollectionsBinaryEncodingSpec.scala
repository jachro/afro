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
        _.map(i => java.lang.Integer.valueOf(i)).toList.asJava,
        """{"type": "array", "items": "int"}"""
      ).toBin
      actual.toBin shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe v
    }
