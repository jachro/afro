package io.jachro.afro

import io.jachro.afro.Schema.Type
import org.apache.avro.util.Utf8
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.*

class MapsBinaryEncodingSpec extends BinaryEncodingSpec with Generators:

  it should "serialize/deserialize a Map - case with a positive count" in:

    val schema = Schema.Type.MapType(name = "field", Schema.Type.IntType.typeOnly)

    forAll { (map: Map[String, Int]) =>
      val actual = AvroEncoder(schema).encode(map).value
      val expected = expectedFrom[Map[String, Int]](
        map,
        _.asJava,
        """{"type": "map", "values": "int"}"""
      )
      actual shouldBe expected

      AvroDecoder(schema).decode(actual).value shouldBe map
    }

  it should "serialize/deserialize a Map - case with a negative count" in:

    val blockSize = minBinaryBlockSize + 1
    val map = Gen.mapOfN(blockSize + 2, booleanMapEntryGen).generateOne

    given TypeEncoder[Map[String, Boolean]] = blockingMapEncoder(blockSize)
    val schema = Schema.Type.MapType(name = "field", Schema.Type.BooleanType.typeOnly)
    val encoded = AvroEncoder(schema).encode(map).value
    val decodedOfficial = readWithOfficialLib[java.util.HashMap[Utf8, Boolean]](
      encoded,
      """{"type": "map", "values": "boolean"}"""
    )
    decodedOfficial.headOption.value.asScala
      .map { case (k, v) => k.toString -> v } shouldBe map

    AvroDecoder(schema).decode(encoded).value shouldBe map

  it should "serialize/deserialize a Map of non-primitive types" in:

    val schema = Schema.Type.MapType(name = "field", TestType.schema)

    val map = Map("field1" -> TestType("tt1", 1), "field2" -> TestType("tt2", 2))

    val encoded = AvroEncoder(schema).encode(map).value

    AvroDecoder(schema).decode(encoded).value shouldBe map

    val encodedWithOfficialLib = expectedFrom[Map[String, TestType]](
      map,
      _.view.mapValues(TestType.avroLibEncoder).toMap.asJava,
      s"""{"type": "map", "values": ${TestType.avroSchema}}"""
    )
    encoded shouldBe encodedWithOfficialLib

  private lazy val booleanMapEntryGen =
    for
      k <- Gen.alphaNumStr
      v <- Arbitrary.arbBool.arbitrary
    yield k -> v
