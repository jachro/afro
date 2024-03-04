package io.renku.avro4s

import io.renku.avro4s.TypeEncoder.TypeEncodingResult
import scodec.bits.ByteVector

trait TypeEncoder[A]:
  self =>

  def encodeValue(v: A): TypeEncodingResult

  def contramap[B](f: B => A): TypeEncoder[B] =
    TypeEncoder.instance[B](b => self.encodeValue(f(b)))

object TypeEncoder:

  type TypeEncodingResult = Either[AvroEncodingException, ByteVector]
  object TypeEncodingResult:
    def success(bv: ByteVector): TypeEncodingResult = Right(bv)

  def apply[A](using enc: TypeEncoder[A]): TypeEncoder[A] = enc

  def instance[A](enc: A => TypeEncodingResult): TypeEncoder[A] =
    (v: A) => enc(v)
