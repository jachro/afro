package io.renku.avro4s

import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Parser as AvroParser
import org.apache.avro.generic.GenericRecord
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector

trait BinaryEncodingSpec
    extends AnyFlatSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues:

  protected def expectedFrom[A](
      values: A,
      encoder: A => Any,
      schema: String
  ): ByteVector =
    expectedFromSeq[A](Seq(values), encoder, schema)

  protected def expectedFromSeq[A](
      values: Seq[A],
      encoder: A => Any,
      schema: String
  ): ByteVector =
    AvroWriter(parse(schema)).write(values, encoder)

  protected def parse: String => AvroSchema =
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
