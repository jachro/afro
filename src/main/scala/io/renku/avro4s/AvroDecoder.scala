package io.renku.avro4s

import scodec.bits.ByteVector

class AvroDecoder[S <: Schema](val schema: S)(using ValueDecoder[schema.valueType]):

  def decode(bytes: ByteVector): Either[AvroDecodingException, schema.valueType] =
    ValueDecoder[schema.valueType].decode(bytes).map(_._1)
