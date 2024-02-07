package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteVector

trait ValueDecoder[A]:
  self =>

  def decode(bytes: ByteVector): Either[AvroDecodingException, (A, ByteVector)]

  def map[B](f: A => B): ValueDecoder[B] =
    (bytes: ByteVector) => self.decode(bytes).map { case (a, bv) => f(a) -> bv }

  def emap[B](f: A => Either[AvroDecodingException, B]): ValueDecoder[B] =
    (bytes: ByteVector) =>
      self.decode(bytes).flatMap { case (a, bv) => f(a).tupleRight(bv) }

object ValueDecoder:
  def apply[A](using enc: ValueDecoder[A]): ValueDecoder[A] = enc
