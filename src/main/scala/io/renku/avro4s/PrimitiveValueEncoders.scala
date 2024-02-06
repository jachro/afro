package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs.*

import scala.annotation.tailrec

trait PrimitiveValueEncoders:

  given ValueEncoder[Null] =
    ValueEncoder.instance(_ => ByteVector.empty.asRight[AvroEncodingException])

  given ValueEncoder[Boolean] = ValueEncoder.instance { v =>
    ByteVector.fromByte((if v then 1 else 0).toByte).asRight[AvroEncodingException]
  }

  given ValueEncoder[Int] = ValueEncoder[Long].contramap(_.toLong)

  given ValueEncoder[Long] = ValueEncoder.instance[Long] { v =>

    @tailrec
    def to7BitsChunks(in: BitVector, res: List[BitVector] = Nil): List[BitVector] =
      if in.bytes.dropWhile(_ == 0).isEmpty then res.reverse
      else if !in.head && in.sizeLessThanOrEqual(7) then in.padRight(7) :: res
      else
        val last7 = in.takeRight(7)
        val head = in.dropRight(7)
        to7BitsChunks(head, last7 :: res)

    @tailrec
    def addMBS(in: List[BitVector], res: List[BitVector] = Nil): List[BitVector] =
      in match
        case Nil       => List(BitVector.low(8))
        case h :: Nil  => (h.padLeft(8) :: res).reverse
        case h :: tail => addMBS(tail, true +: h :: res)

    val zigZag = if v >= 0 then 2 * v else 2 * v.abs - 1
    val zigZagEnc = int64.encode(zigZag)

    zigZagEnc.toEither
      .leftMap(err => AvroEncodingException(err.messageWithContext))
      .map { zigZagV =>
        val chunks = to7BitsChunks(zigZagV)
        val withMbs = addMBS(chunks)
        withMbs.reduce(_ ++ _).toByteVector
      }
  }
