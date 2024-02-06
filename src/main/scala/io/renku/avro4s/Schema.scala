package io.renku.avro4s

sealed trait Schema[A]

object Schema:

  object Type:
    final case class Null(name: String) extends Schema[scala.Null]
    final case class Boolean(name: String) extends Schema[scala.Boolean]
    final case class Int(name: String) extends Schema[scala.Int]
    final case class Long(name: String) extends Schema[scala.Long]
    final case class Float(name: String) extends Schema[scala.Int]
