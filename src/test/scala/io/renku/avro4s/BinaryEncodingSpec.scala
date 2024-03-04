package io.renku.avro4s

import io.renku.avro4s.all
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Parser as AvroParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{EitherValues, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector

trait BinaryEncodingSpec
    extends AnyFlatSpec
    with all
    with ScalaCheckPropertyChecks
    with should.Matchers
    with EitherValues
    with OptionValues:

  protected def binaryWriter: String => AvroWriter =
    parse.andThen(AvroWriter.apply)
  protected def binaryBlockingWriter: String => AvroWriter =
    parse.andThen(AvroWriter.blocking)

  protected def parse: String => AvroSchema =
    new AvroParser().parse

  protected def expectedFrom[A](
      values: A,
      encoder: A => Any,
      schema: String,
      writerFactory: String => AvroWriter = binaryWriter
  ): ByteVector =
    expectedFromSeq[A](Seq(values), encoder, schema, writerFactory)

  protected def expectedFromSeq[A](
      values: Seq[A],
      encoder: A => Any,
      schema: String,
      writerFactory: String => AvroWriter = binaryWriter
  ): ByteVector =
    writerFactory(schema).write(values, encoder)

  protected def readWithOfficialLib[I](
      bv: ByteVector,
      schema: String
  ): Seq[I] =
    AvroReader(parse(schema))
      .read(bv)
      .map(_.asInstanceOf[I])
