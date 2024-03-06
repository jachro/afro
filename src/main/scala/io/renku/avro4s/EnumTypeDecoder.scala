package io.renku.avro4s

import scala.language.reflectiveCalls

trait EnumTypeDecoder extends PrimitiveTypeDecoders:

  def enumTypeDecoder[E](factory: { def fromOrdinal(ordinal: Int): E }): TypeDecoder[E] =
    TypeDecoder[Int].map[E](factory.fromOrdinal)
