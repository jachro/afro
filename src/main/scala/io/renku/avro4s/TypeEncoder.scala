package io.renku.avro4s

import scodec.bits.ByteVector

trait TypeEncoder[A]:
  self =>

  def encodeValue(v: A): Either[AvroEncodingException, ByteVector]

  def contramap[B](f: B => A): TypeEncoder[B] =
    TypeEncoder.instance[B](b => self.encodeValue(f(b)))

object TypeEncoder:

  def apply[A](using enc: TypeEncoder[A]): TypeEncoder[A] = enc

  def instance[A](enc: A => Either[AvroEncodingException, ByteVector]): TypeEncoder[A] =
    (v: A) => enc(v)
