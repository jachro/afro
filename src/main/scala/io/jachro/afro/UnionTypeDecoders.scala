package io.jachro.afro

import cats.syntax.all.*
import io.jachro.afro.TypeDecoder.Outcome
import scodec.bits.ByteVector

trait UnionTypeDecoders extends PrimitiveTypeDecoders:

  given optionalDecoder[V](using vDec: TypeDecoder[V]): TypeDecoder[Option[V]] =
    (bv: ByteVector) =>
      TypeDecoder[Int].decode(bv).flatMap {
        case Outcome(0, lb) =>
          Outcome(Option.empty[V], lb).asRight[AvroDecodingException]
        case Outcome(1, lb) =>
          vDec.decode(lb).map(o => o.copy(value = Option(o.value)))
        case Outcome(_, _) =>
          AvroDecodingException("Option cannot have items with index more than 1")
            .asLeft[Outcome[Option[V]]]
      }
