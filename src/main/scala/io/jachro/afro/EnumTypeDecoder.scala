package io.jachro.afro

import scala.reflect.Selectable.reflectiveSelectable

trait EnumTypeDecoder extends PrimitiveTypeDecoders:

  def enumTypeDecoder[E](factory: { def fromOrdinal(ordinal: Int): E }): TypeDecoder[E] =
    TypeDecoder[Int].map[E](factory.fromOrdinal)
