package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteVector

trait TypeDecoder[A]:
  self =>

  def decode(bytes: ByteVector): Either[AvroDecodingException, (A, ByteVector)]

  def map[B](f: A => B): TypeDecoder[B] =
    (bytes: ByteVector) => self.decode(bytes).map { case (a, bv) => f(a) -> bv }

  def emap[B](f: A => Either[AvroDecodingException, B]): TypeDecoder[B] =
    (bytes: ByteVector) =>
      self.decode(bytes).flatMap { case (a, bv) => f(a).tupleRight(bv) }

object TypeDecoder:

  type TypeDecodingResult[A] = Either[AvroDecodingException, (A, ByteVector)]

  def apply[A](using enc: TypeDecoder[A]): TypeDecoder[A] = enc

  def instance[A](f: ByteVector => TypeDecodingResult[A]): TypeDecoder[A] =
    (bytes: ByteVector) => f(bytes)
