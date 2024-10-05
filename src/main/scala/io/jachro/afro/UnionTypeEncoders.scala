package io.jachro.afro

import cats.syntax.all.*
import scodec.bits.ByteVector

trait UnionTypeEncoders extends PrimitiveTypeEncoders:

  given optionalEncoder[V](using vEnc: TypeEncoder[V]): TypeEncoder[Option[V]] =
    TypeEncoder.instance {
      case None =>
        TypeEncoder[Int].encodeValue(0) // 0-based idx in the type array
      case Some(v) =>
        List(
          TypeEncoder[Int].encodeValue(1), // 0-based idx in the type array
          vEnc.encodeValue(v)
        ).sequence.map(_.reduce(_ ++ _))
    }
