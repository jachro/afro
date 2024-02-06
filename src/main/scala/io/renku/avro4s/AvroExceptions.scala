package io.renku.avro4s

case class AvroDecodingException(message: String) extends RuntimeException(message)
case class AvroEncodingException(message: String) extends RuntimeException(message)
