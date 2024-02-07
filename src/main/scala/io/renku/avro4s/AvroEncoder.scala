package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteVector

object AvroEncoder:

  def encode[A: ValueEncoder](
      value: A,
      schema: Schema[A]
  ): Either[AvroEncodingException, ByteVector] =
    (value, schema) match
      case (v, s: Schema.Type.NullType)              => ValueEncoder[A].encodeValue(v)
      case (v: Boolean, s: Schema.Type.BooleanType)  => ValueEncoder[A].encodeValue(v)
      case (v: Int, s: Schema.Type.IntType)          => ValueEncoder[A].encodeValue(v)
      case (v: Long, s: Schema.Type.LongType)        => ValueEncoder[A].encodeValue(v)
      case (v: Float, s: Schema.Type.FloatType)      => ValueEncoder[A].encodeValue(v)
      case (v: Double, s: Schema.Type.DoubleType)    => ValueEncoder[A].encodeValue(v)
      case (v: ByteVector, s: Schema.Type.BytesType) => ValueEncoder[A].encodeValue(v)
      case (v, s) =>
        AvroEncodingException(s"Value of type ${v.getClass} cannot be encoded with $s")
          .asLeft[ByteVector]
