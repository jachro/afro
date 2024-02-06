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
      case s: Schema.Type.Null    => ValueDecoder[Null].decode(bytes).map(_._1)
      case s: Schema.Type.Boolean => ValueDecoder[Boolean].decode(bytes).map(_._1)
      case s: Schema.Type.Int     => ValueDecoder[Int].decode(bytes).map(_._1)
      case s: Schema.Type.Long    => ValueDecoder[Long].decode(bytes).map(_._1)
