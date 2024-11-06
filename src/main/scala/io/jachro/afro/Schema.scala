package io.jachro.afro

import scodec.bits.ByteVector

sealed trait Schema:
  type objectType
  val `type`: String

object Schema:

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
      addField(Record.Field(name, schema, default = Option.empty))
    def addOptionalField[V](name: String, valueSchema: Schema): Record[A] =
      addField(Record.Field.optional[V](name, valueSchema))
    def addEitherField[L, R](
        name: String,
        lSchema: Schema,
        rSchema: Schema,
        leftDefault: L
    ): Record[A] =
      addField(Record.Field.either[L, R](name, lSchema, rSchema, leftDefault))

  object Record:
    def apply[A](name: String): Record[A] = Record(name, Seq.empty)

    sealed trait Field:
      val name: String
      val schema: Schema
      val default: Option[?]
    object Field:
      def apply(
          name: String,
          schema: Schema,
          default: Option[schema.objectType]
      ): Field = Simple(name, schema, default)
      def optional[V](
          name: String,
          valueSchema: Schema
      ): Field = Union.Optional[V](name, valueSchema)
      def either[L, R](
          name: String,
          lSchema: Schema,
          rSchema: Schema,
          leftDefault: L
      ): Field = Union.Either[L, R](name, lSchema, rSchema, leftDefault)

      final class Simple(
          override val name: String,
          override val schema: Schema,
          override val default: Option[schema.objectType]
      ) extends Field

      sealed abstract class Union[V](name: String) extends Field
      object Union:
        final case class Optional[V](name: String, valueSchema: Schema)
            extends Union[Option[V]](name):

          override val schema: Schema = new Schema:
            override type objectType = Option[V]
            override val `type`: String = s"""[ "null", "${valueSchema.`type`}" ]"""

          override val default: Option[valueSchema.objectType] =
            Option.empty[valueSchema.objectType]

        final case class Either[L, R](
            name: String,
            lSchema: Schema,
            rSchema: Schema,
            leftDefault: L
        ) extends Union[scala.util.Either[L, R]](name):

          override val schema: Schema = new Schema:
            override type objectType = scala.util.Either[L, R]
            override val `type`: String =
              s"""[ "${lSchema.`type`}", "${rSchema.`type`}" ]"""

          override val default: Option[scala.util.Left[L, R]] =
            Option(scala.util.Left(leftDefault))

  final case class EnumType[A <: scala.reflect.Enum](
      maybeName: Option[String],
      symbols: scala.Array[A]
  ) extends Schema:
    override type objectType = A
    override val `type`: String = "enum"
  object EnumType:
    def apply[A <: scala.reflect.Enum](
        name: String,
        symbols: scala.Array[A]
    ): EnumType[A] =
      EnumType[A](Some(name), symbols)

  sealed abstract class GenericArray[I](
      name: String,
      itemsSchema: Schema { type objectType = I }
  ) extends Schema:
    override val `type`: String = "array"

  object Array:

    def apply[I](
        name: String,
        itemsSchema: Schema { type objectType = I }
    ): Iterable[I, scala.Array] =
      Iterable[I, scala.Array](name, itemsSchema)

    def forList[I](
        name: String,
        itemsSchema: Schema { type objectType = I }
    ): Iterable[I, List] =
      backedBy[List, I](name, itemsSchema)

    def backedBy[C[X] <: scala.Iterable[X], I](
        name: String,
        itemsSchema: Schema { type objectType = I }
    ): Iterable[I, C] =
      Iterable[I, C](name, itemsSchema)

    final case class Iterable[I, C[X]](
        name: String,
        itemsSchema: Schema { type objectType = I }
    ) extends GenericArray[I](name, itemsSchema):
      override type objectType = C[I]

  final case class MapType[I](
      name: String,
      valuesSchema: Schema { type objectType = I }
  ) extends Schema:
    override type objectType = scala.collection.Map[String, I]
    override val `type`: String = "map"

  final case class FixedType[A](name: String, size: Int) extends Schema:
    override type objectType = A
    override val `type`: String = "fixed"
