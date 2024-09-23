package io.jachro.afro

import io.jachro.afro.TypeDecoder.Outcome
import scodec.bits.ByteVector

import scala.reflect.ClassTag

trait MapDecoders extends CollectionDecoders with PrimitiveTypeDecoders:

  given [M[X] <: collection.Map[String, X], I](using
      vd: TypeDecoder[I],
      converter: Map[String, I] => M[I],
      ict: ClassTag[I]
  ): TypeDecoder[M[I]] =
    given TypeDecoder[(String, I)] = entryDecoder[I]
    TypeDecoder
      .instance(decodeList[(String, I)](List.empty[(String, I)], _))
      .map(_.toMap)
      .map(converter)

  private def entryDecoder[I]: TypeDecoder[I] ?=> TypeDecoder[(String, I)] =
    vd ?=>
      TypeDecoder.instance[(String, I)] { bv =>
        for
          Outcome(k, klb) <- TypeDecoder[String].decode(bv)
          Outcome(v, vlb) <- vd.decode(klb)
        yield Outcome(k -> v, vlb)
      }
