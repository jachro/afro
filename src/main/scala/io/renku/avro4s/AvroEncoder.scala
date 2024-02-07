package io.renku.avro4s

import scodec.bits.ByteVector

class AvroEncoder[S <: Schema](val schema: S)(using ValueEncoder[schema.valueType]):

  def encode(value: schema.valueType): Either[AvroEncodingException, ByteVector] =
    ValueEncoder[schema.valueType].encodeValue(value)
