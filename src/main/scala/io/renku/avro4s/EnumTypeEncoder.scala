package io.renku.avro4s

trait EnumTypeEncoder extends PrimitiveTypeEncoders:

  given enumTypeEncoder[E <: scala.reflect.Enum]: TypeEncoder[E] =
    TypeEncoder[Int].contramap[E](_.ordinal)
