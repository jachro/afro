package io.jachro.afro

import scodec.bits.ByteVector

class AvroEncoder[S <: Schema](val schema: S)(using TypeEncoder[schema.objectType]):

  def encode(value: schema.objectType): Either[AvroEncodingException, ByteVector] =
    TypeEncoder[schema.objectType].encodeValue(value)
