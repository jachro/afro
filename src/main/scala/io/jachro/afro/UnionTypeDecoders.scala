package io.jachro.afro

import cats.syntax.all.*
import io.jachro.afro.TypeDecoder.Outcome
import scodec.bits.ByteVector

trait UnionTypeDecoders extends PrimitiveTypeDecoders:

  given optionalDecoder[V](using vDec: TypeDecoder[V]): TypeDecoder[Option[V]] =
    (bv: ByteVector) =>
      TypeDecoder[Int]
        .decode(bv)
        .flatMap {
          case Outcome(0, lb) =>
            Outcome(Option.empty[V], lb).asRight[AvroDecodingException]
          case Outcome(1, lb) =>
            vDec.decode(lb).map(o => o.copy(value = Option(o.value)))
          case Outcome(_, _) =>
            AvroDecodingException("Option cannot have items with index more than 1")
              .asLeft[Outcome[Option[V]]]
        }
        .handleError(_ => Outcome(Option.empty[V], bv))

  def eitherDecoder[L, R](default: L)(using
      lDec: TypeDecoder[L],
      rDec: TypeDecoder[R]
  ): TypeDecoder[Either[L, R]] =
    (bv: ByteVector) =>
      TypeDecoder[Int]
        .decode(bv)
        .flatMap {
          case Outcome(0, lb) =>
            lDec.decode(lb).map(o => o.copy(value = Left[L, R](o.value)))
          case Outcome(1, lb) =>
            rDec.decode(lb).map(o => o.copy(value = Right[L, R](o.value)))
          case Outcome(r, _) =>
            println(s"in here $r")
            AvroDecodingException("Either cannot have items with index more than 1")
              .asLeft[Outcome[Either[L, R]]]
        }
        .handleError(_ => Outcome(default.asLeft, bv))
