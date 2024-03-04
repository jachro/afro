package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.TypeDecoder.{Outcome, Result}
import scodec.bits.ByteVector

trait TypeDecoder[A]:
  self =>

  def decode(bytes: ByteVector): Result[A]

  def map[B](f: A => B): TypeDecoder[B] =
    (bytes: ByteVector) =>
      self.decode(bytes).map { case Outcome(a, bv) => Outcome(f(a), bv) }

  def emap[B](f: A => Result[B]): TypeDecoder[B] =
    (bytes: ByteVector) =>
      self.decode(bytes).flatMap { case Outcome(a, bv) =>
        f(a).map(far => Outcome(far.value, bv))
      }

object TypeDecoder:

  final case class Outcome[A](value: A, leftBytes: ByteVector)

  type Result[A] = Either[AvroDecodingException, Outcome[A]]

  extension [A](result: Result[A])

    def flatMap[B](f: Outcome[A] => Result[B]): Result[B] =
      result.flatMap(f)

    def >>=[B](f: Outcome[A] => Result[B]): Result[B] =
      flatMap(f)

  object Result:
    def success[A](outcome: Outcome[A]): Result[A] =
      outcome.asRight
    def success[A](res: A, bv: ByteVector): Result[A] =
      success(Outcome(res, bv))

    def failure[A](cause: AvroDecodingException): Result[A] =
      cause.asLeft
    def failure[A](message: String): Result[A] =
      failure(AvroDecodingException(message))

  def apply[A](using enc: TypeDecoder[A]): TypeDecoder[A] = enc

  def instance[A](f: ByteVector => Result[A]): TypeDecoder[A] =
    (bytes: ByteVector) => f(bytes)
