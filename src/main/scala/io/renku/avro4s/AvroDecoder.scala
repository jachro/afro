package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.Schema.Type
import scodec.bits.ByteVector

object AvroDecoder:

  def decode[A: ValueDecoder](
      bytes: ByteVector,
      schema: Schema[A]
  ): Either[AvroDecodingException, A] =
    schema match
      case s: Schema.Type.NullType    => ValueDecoder[Null].decode(bytes).map(_._1)
      case s: Schema.Type.BooleanType => ValueDecoder[Boolean].decode(bytes).map(_._1)
      case s: Schema.Type.IntType     => ValueDecoder[Int].decode(bytes).map(_._1)
      case s: Schema.Type.LongType    => ValueDecoder[Long].decode(bytes).map(_._1)
      case s: Schema.Type.FloatType   => ValueDecoder[Float].decode(bytes).map(_._1)
      case s: Schema.Type.DoubleType  => ValueDecoder[Double].decode(bytes).map(_._1)
      case s: Schema.Type.BytesType   => ValueDecoder[ByteVector].decode(bytes).map(_._1)
