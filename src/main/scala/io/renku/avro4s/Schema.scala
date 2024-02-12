package io.renku.avro4s

import scodec.bits.ByteVector

sealed trait Schema:
  type objectType
  val `type`: String

object Schema:

  object Type:
    final case class NullType(maybeName: Option[String]) extends Schema:
      override type objectType = scala.Null
      override val `type`: String = "null"
    object NullType:
      val typeOnly: NullType = NullType(None)
      def apply(): NullType = typeOnly
      def apply(name: String): NullType = NullType(Some(name))

    final case class BooleanType(maybeName: Option[String]) extends Schema:
      override type objectType = scala.Boolean
      override val `type`: String = "boolean"
    object BooleanType:
      val typeOnly: BooleanType = BooleanType(None)
      def apply(): BooleanType = typeOnly
      def apply(name: String): BooleanType = BooleanType(Some(name))

    final case class IntType(maybeName: Option[String]) extends Schema:
      override type objectType = scala.Int
      override val `type`: String = "int"
    object IntType:
      val typeOnly: IntType = IntType(None)
      def apply(): IntType = typeOnly
      def apply(name: String): IntType = IntType(Some(name))

    final case class LongType(maybeName: Option[String]) extends Schema:
      override type objectType = scala.Long
      override val `type`: String = "long"
    object LongType:
      val typeOnly: LongType = LongType(None)
      def apply(): LongType = typeOnly
      def apply(name: String): LongType = LongType(Some(name))

    final case class FloatType(maybeName: Option[String]) extends Schema:
      override type objectType = scala.Float
      override val `type`: String = "float"
    object FloatType:
      val typeOnly: StringType = StringType(None)
      def apply(): FloatType = FloatType(None)
      def apply(name: String): FloatType = FloatType(Some(name))

    final case class DoubleType(maybeName: Option[String]) extends Schema:
      override type objectType = scala.Double
      override val `type`: String = "double"
    object DoubleType:
      def apply(): DoubleType = DoubleType(None)
      def apply(name: String): DoubleType = DoubleType(Some(name))

    final case class BytesType(maybeName: Option[String]) extends Schema:
      override type objectType = ByteVector
      override val `type`: String = "bytes"
    object BytesType:
      def apply(): BytesType = BytesType(None)
      def apply(name: String): BytesType = BytesType(Some(name))

    final case class StringType(maybeName: Option[String]) extends Schema:
      override type objectType = String
      override val `type`: String = "string"
    object StringType:
      val typeOnly: StringType = StringType(None)
      def apply(): StringType = typeOnly
      def apply(name: String): StringType = StringType(Some(name))

    final case class Record[A](name: String, fields: Seq[Record.Field]) extends Schema:
      override type objectType = A
      override val `type`: String = "record"

      def addField(field: Record.Field): Record[A] =
        copy(fields = (field +: fields).reverse)

      def addField(name: String, schema: Schema): Record[A] =
        addField(Record.Field(name, schema))

    object Record:

      def apply[A](name: String): Record[A] = Record(name, Seq.empty)

      final case class Field(name: String, schema: Schema)
