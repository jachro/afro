package io.jachro.afro

import io.jachro.afro.TypeEncoder.Result
import scodec.bits.ByteVector

trait TypeEncoder[A]:
  self =>

  def encodeValue(v: A): Result

  def contramap[B](f: B => A): TypeEncoder[B] =
    TypeEncoder.instance[B](b => self.encodeValue(f(b)))

object TypeEncoder:

  type Result = Either[AvroEncodingException, ByteVector]
  object Result:
    def success(bv: ByteVector): Result = Right(bv)

  def apply[A](using enc: TypeEncoder[A]): TypeEncoder[A] = enc

  def instance[A](enc: A => Result): TypeEncoder[A] =
    (v: A) => enc(v)
