package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteVector

object AvroEncoder:

  def encode[A: ValueEncoder](
      value: A,
      schema: Schema[A]
  ): Either[AvroEncodingException, ByteVector] =
    (value, schema) match
      case (v, s: Schema.Type.Null)             => ValueEncoder[A].encodeValue(v)
      case (v: Boolean, s: Schema.Type.Boolean) => ValueEncoder[A].encodeValue(v)
      case (v: Int, s: Schema.Type.Int)         => ValueEncoder[A].encodeValue(v)
      case (v: Long, s: Schema.Type.Long)       => ValueEncoder[A].encodeValue(v)
      case (v: Float, s: Schema.Type.Float)     => ValueEncoder[A].encodeValue(v)
      case (v: Double, s: Schema.Type.Double)   => ValueEncoder[A].encodeValue(v)
      case (v, s) =>
        AvroEncodingException(s"Value of type ${v.getClass} cannot be encoded with $s")
          .asLeft[ByteVector]
