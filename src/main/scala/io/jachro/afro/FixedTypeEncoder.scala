package io.jachro.afro

import scodec.bits.ByteVector

trait FixedTypeEncoder:

  def fixedTypeEncoder[A](toByteVector: A => ByteVector): TypeEncoder[A] =
    TypeEncoder.instance[A](v => TypeEncoder.Result.success(toByteVector(v)))
