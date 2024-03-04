package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.TypeDecoder.TypeDecodingResult
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.reflect.ClassTag

trait CollectionDecoders extends PrimitiveTypeDecoders:

  given [C[X], I](using
      ie: TypeDecoder[I],
      converter: Array[I] => C[I],
      ict: ClassTag[I]
  ): TypeDecoder[C[I]] =
    TypeDecoder
      .instance[Array[I]](decodeArray[I](Array.empty[I], _))
      .map[C[I]](converter)

  given fromArray[I](using
      ct: ClassTag[Array[I]],
      ict: ClassTag[I]
  ): Function1[Array[I], Array[I]] =
    identity
  given [I](using ct: ClassTag[Set[I]], ict: ClassTag[I]): Function1[Array[I], Set[I]] =
    _.toSet
  given [I](using ct: ClassTag[List[I]], ict: ClassTag[I]): Function1[Array[I], List[I]] =
    _.toList

  private def decodeArray[I](result: Array[I], bv: ByteVector)(using
      ie: TypeDecoder[I],
      ct: ClassTag[I]
  ): TypeDecodingResult[Array[I]] =
    decodeCount(bv).flatMap {
      case (0L, bv) =>
        TypeDecodingResult.success(result, bv)
      case (count, bv) if count > 0 =>
        decodeBlockItems[I](count, bv)
          .flatMap { case (curr, restBv) => decodeArray[I](result ++ curr, restBv) }
      case (count, bv) =>
        decodeBlock[I](count, bv)
          .flatMap { case (curr, restBv) => decodeArray[I](result ++ curr, restBv) }
    }

  private lazy val decodeCount: ByteVector => TypeDecodingResult[Long] =
    TypeDecoder[Long].decode

  private def decodeBlockItems[I](count: Long, bv: ByteVector)(using
      ie: TypeDecoder[I],
      ct: ClassTag[I]
  ): TypeDecodingResult[Array[I]] =
    decodeItems(count, (List.empty[I] -> bv).asRight)
      .map { case (l, bv) => l.reverse.toArray -> bv }

  private def decodeBlock[I](count: Long, bv: ByteVector)(using
      ie: TypeDecoder[I],
      ct: ClassTag[I]
  ): TypeDecodingResult[Array[I]] =
    for
      bv <- decodeBlockSize(bv).map((_, bv) => bv)
      res <- decodeBlockItems(count * -1, bv)
    yield res

  private def decodeBlockSize(bv: ByteVector): TypeDecodingResult[Long] =
    TypeDecoder[Long]
      .decode(bv)
      .flatMap {
        case (blockSize, bv) if bv.size < blockSize =>
          AvroDecodingException(
            "Expected array block size < number of available bytes"
          ).asLeft
        case tuple =>
          tuple.asRight[AvroDecodingException]
      }

  @tailrec
  private def decodeItems[I](
      leftItems: Long,
      res: TypeDecodingResult[List[I]]
  )(using ie: TypeDecoder[I]): TypeDecodingResult[List[I]] =
    if res.isLeft then res
    else if leftItems == 0L then res
    else
      val newRes = res.flatMap { case (list, bv) =>
        ie.decode(bv)
          .map { case (di, rest) => (di :: list) -> rest }
      }
      decodeItems(leftItems - 1, newRes)
