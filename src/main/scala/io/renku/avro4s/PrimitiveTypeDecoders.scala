package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.TypeDecoder.{Outcome, Result}
import scodec.bits.ByteOrdering.LittleEndian
import scodec.bits.{BitVector, ByteVector}

import scala.annotation.tailrec

trait PrimitiveTypeDecoders:

  val nullTypeDecoder: TypeDecoder[Null] = (bytes: ByteVector) =>
    TypeDecoder.Result.success(null, bytes)

  given TypeDecoder[Boolean] = (bytes: ByteVector) =>
    bytes.splitAt(1) match
      case (ByteVector.empty, _) =>
        TypeDecoder.Result.failure("Cannot decode boolean value from empty bytes")
      case (l, leftBytes) =>
        val res = if l.head == 1 then true else false
        TypeDecoder.Result.success(res, leftBytes)

  given TypeDecoder[Int] = TypeDecoder[Long].map(_.toInt)

  given TypeDecoder[Long] = (bytes: ByteVector) =>

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

    TypeDecoder.Result.success(res, rest)

  given TypeDecoder[Float] = (bytes: ByteVector) =>
    bytes.splitAt(4) match
      case (ByteVector.empty, _) =>
        TypeDecoder.Result.failure("Cannot decode float value from empty bytes")
      case (l, r) =>
        val d = java.lang.Float
          .intBitsToFloat(l.toInt(ordering = LittleEndian))
          .floatValue
        TypeDecoder.Result.success(d, r)

  given TypeDecoder[Double] = (bytes: ByteVector) =>
    bytes.splitAt(8) match
      case (ByteVector.empty, _) =>
        TypeDecoder.Result.failure("Cannot decode double value from empty bytes")
      case (l, r) =>
        val d = java.lang.Double
          .longBitsToDouble(l.toLong(ordering = LittleEndian))
          .doubleValue
        TypeDecoder.Result.success(d, r)

  given TypeDecoder[ByteVector] = {
    case ByteVector.empty =>
      TypeDecoder.Result.failure("Cannot decode bytes value from empty bytes")
    case bytes =>
      TypeDecoder[Long].decode(bytes) >>= {
        case out if out.leftBytes.size < out.value =>
          TypeDecoder.Result.failure(
            s"Cannot decode bytes value as there's only ${out.leftBytes.size} while expected ${out.value}"
          )
        case Outcome(size, leftBytes) =>
          TypeDecoder.Result
            .success(leftBytes.take(size), leftBytes.drop(size))
      }
  }

  given TypeDecoder[String] = {
    case ByteVector.empty =>
      AvroDecodingException("Cannot decode string value from empty bytes").asLeft
    case bytes =>
      TypeDecoder[Long].decode(bytes) >>= {
        case Outcome(size, leftBytes) if leftBytes.size < size =>
          TypeDecoder.Result.failure(
            s"Cannot decode string value as there's only ${leftBytes.size} while expected $size"
          )
        case Outcome(size, leftBytes) =>
          leftBytes
            .take(size)
            .decodeUtf8
            .leftMap(
              AvroDecodingException(s"Cannot decode string value using UTF-8 charset", _)
            )
            .map(Outcome(_, leftBytes.drop(size)))
      }
  }
