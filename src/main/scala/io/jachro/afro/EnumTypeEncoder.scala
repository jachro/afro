package io.jachro.afro

trait EnumTypeEncoder extends PrimitiveTypeEncoders:

  given enumTypeEncoder[E <: scala.reflect.Enum]: TypeEncoder[E] =
    TypeEncoder[Int].contramap[E](_.ordinal)
