package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteOrdering.LittleEndian
import scodec.bits.{BitVector, ByteVector}

import scala.annotation.tailrec

trait PrimitiveValueDecoders:

  given ValueDecoder[Null] = (bytes: ByteVector) =>
    (null, bytes).asRight[AvroDecodingException]

  given ValueDecoder[Boolean] = (bytes: ByteVector) =>
    bytes.splitAt(1) match
      case (ByteVector.empty, _) =>
        AvroDecodingException("Cannot decode boolean value from empty bytes").asLeft
      case (l, r) =>
        (if l.head == 1 then true else false, r).asRight[AvroDecodingException]

  given ValueDecoder[Int] = ValueDecoder[Long].emap(_.toInt)

  given ValueDecoder[Long] = (bytes: ByteVector) =>

    @tailrec
    def takeNumberBits(bytes: ByteVector, acc: BitVector): (BitVector, ByteVector) =
      val (head, tail) = bytes.splitAt(1)
      val bits = head.toBitVector
      if bits.headOption.contains(false) then bits.drop(1) ++ acc -> tail
      else takeNumberBits(tail, bits.drop(1) ++ acc)

    val (number, rest) = takeNumberBits(bytes, BitVector.empty)

    val zigZag = number.toLong(signed = false)

    val res =
      if (zigZag % 2 == 0) zigZag / 2
      else (zigZag + 1) / 2 * -1

    (res -> rest).asRight

  given ValueDecoder[Float] = (bytes: ByteVector) =>
    bytes.splitAt(4) match
      case (ByteVector.empty, _) =>
        AvroDecodingException("Cannot decode float value from empty bytes").asLeft
      case (l, r) =>
        val d = java.lang.Float
          .intBitsToFloat(l.toInt(ordering = LittleEndian))
          .floatValue
        (d, r).asRight[AvroDecodingException]

  given ValueDecoder[Double] = (bytes: ByteVector) =>
    bytes.splitAt(8) match
      case (ByteVector.empty, _) =>
        AvroDecodingException("Cannot decode double value from empty bytes").asLeft
      case (l, r) =>
        val d = java.lang.Double
          .longBitsToDouble(l.toLong(ordering = LittleEndian))
          .doubleValue
        (d, r).asRight[AvroDecodingException]

  given ValueDecoder[ByteVector] = {
    case ByteVector.empty =>
      AvroDecodingException("Cannot decode bytes value from empty bytes").asLeft
    case bytes =>
      ValueDecoder[Long].decode(bytes) >>= {
        case (size, r) if r.size < size =>
          AvroDecodingException(
            s"Cannot decode bytes value as there's only ${r.size} while expected $size"
          ).asLeft
        case (size, r) =>
          (r.take(size) -> r.drop(size)).asRight
      }
  }

  given ValueDecoder[String] = {
    case ByteVector.empty =>
      AvroDecodingException("Cannot decode string value from empty bytes").asLeft
    case bytes =>
      ValueDecoder[Long].decode(bytes) >>= {
        case (size, r) if r.size < size =>
          AvroDecodingException(
            s"Cannot decode string value as there's only ${r.size} while expected $size"
          ).asLeft
        case (size, r) =>
          r.take(size)
            .decodeUtf8
            .leftMap(
              AvroDecodingException(s"Cannot decode string value using UTF-8 charset", _)
            )
            .tupleRight(r.drop(size))
      }
  }
