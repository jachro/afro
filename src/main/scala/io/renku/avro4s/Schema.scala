package io.renku.avro4s

import scodec.bits.ByteVector

sealed trait Schema:
  val name: String
  type valueType
  val `type`: String

object Schema:

  object Type:
    final case class NullType(name: String) extends Schema:
      override type valueType = scala.Null
      override val `type`: String = "null"
    final case class BooleanType(name: String) extends Schema:
      override type valueType = scala.Boolean
      override val `type`: String = "boolean"
    final case class IntType(name: String) extends Schema:
      override type valueType = scala.Int
      override val `type`: String = "int"
    final case class LongType(name: String) extends Schema:
      override type valueType = scala.Long
      override val `type`: String = "long"
    final case class FloatType(name: String) extends Schema:
      override type valueType = scala.Float
      override val `type`: String = "float"
    final case class DoubleType(name: String) extends Schema:
      override type valueType = scala.Double
      override val `type`: String = "double"
    final case class BytesType(name: String) extends Schema:
      override type valueType = ByteVector
      override val `type`: String = "bytes"
    final case class StringType(name: String) extends Schema:
      override type valueType = String
      override val `type`: String = "string"

  final case class Record[A](name: String, fields: Seq[Record.Field]) extends Schema:
    override type valueType = A
    override val `type`: String = "record"

  object Record:
    final case class Field(name: String, `type`: String) extends Schema
