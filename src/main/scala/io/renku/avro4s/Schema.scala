package io.renku.avro4s

import scodec.bits.ByteVector

sealed trait Schema[A]

object Schema:

  object Type:
    final case class NullType(name: String) extends Schema[scala.Null]
    final case class BooleanType(name: String) extends Schema[scala.Boolean]
    final case class IntType(name: String) extends Schema[scala.Int]
    final case class LongType(name: String) extends Schema[scala.Long]
    final case class FloatType(name: String) extends Schema[scala.Float]
    final case class DoubleType(name: String) extends Schema[scala.Double]
    final case class BytesType(name: String) extends Schema[ByteVector]
//    final case class String(name: String) extends Schema[String]
