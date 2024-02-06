package io.renku.avro4s

import scodec.bits.ByteVector

trait ValueEncoder[A]:
  self =>

  def encodeValue(v: A): Either[AvroEncodingException, ByteVector]

  def contramap[B](f: B => A): ValueEncoder[B] =
    ValueEncoder.instance[B](b => self.encodeValue(f(b)))

object ValueEncoder:

  def apply[A](using enc: ValueEncoder[A]): ValueEncoder[A] = enc

  def instance[A](enc: A => Either[AvroEncodingException, ByteVector]): ValueEncoder[A] =
    (v: A) => enc(v)
