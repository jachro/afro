package io.jachro.afro

import io.jachro.afro.TypeDecoder.Result
import scodec.bits.ByteVector

trait FixedTypeDecoder:

  def fixedTypeDecoder[A](
      toFixedType: ByteVector => A,
      schema: Schema.FixedType[A]
  ): TypeDecoder[A] =
    TypeDecoder.instance[A] { bv =>
      val (typeValue, tail) = bv.splitAt(schema.size)
      Result.success(toFixedType(typeValue), tail)
    }
