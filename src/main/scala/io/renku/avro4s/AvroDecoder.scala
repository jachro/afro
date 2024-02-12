package io.renku.avro4s

import scodec.bits.ByteVector

class AvroDecoder[S <: Schema](val schema: S)(using TypeDecoder[schema.objectType]):

  def decode(bytes: ByteVector): Either[AvroDecodingException, schema.objectType] =
    TypeDecoder[schema.objectType].decode(bytes).map(_._1)
