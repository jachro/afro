package io.renku.avro4s

case class AvroDecodingException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

object AvroDecodingException:
  def apply(message: String): AvroDecodingException = AvroDecodingException(message, null)

case class AvroEncodingException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

object AvroEncodingException:
  def apply(message: String): AvroEncodingException = AvroEncodingException(message, null)
