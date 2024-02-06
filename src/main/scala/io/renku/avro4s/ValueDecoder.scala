package io.renku.avro4s

import scodec.bits.ByteVector

trait ValueDecoder[A]:
  self =>

  def decode(bytes: ByteVector): Either[AvroDecodingException, (A, ByteVector)]

  def emap[B](f: A => B): ValueDecoder[B] =
    (bytes: ByteVector) => self.decode(bytes).map { case (a, bv) => f(a) -> bv }

object ValueDecoder:
  def apply[A](using enc: ValueDecoder[A]): ValueDecoder[A] = enc
