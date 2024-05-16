package io.jachro.afro

import io.jachro.afro.TypeDecoder.{Outcome, Result}
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.reflect.ClassTag

trait CollectionDecoders extends PrimitiveTypeDecoders:

  given [C[X], I](using
      ie: TypeDecoder[I],
      converter: List[I] => C[I],
      ict: ClassTag[I]
  ): TypeDecoder[C[I]] =
    TypeDecoder
      .instance[List[I]](decodeList[I](List.empty[I], _))
      .map[C[I]](converter)

  given [I](using ct: ClassTag[Set[I]], ict: ClassTag[I]): Function1[List[I], Set[I]] =
    _.toSet
  given [I](using
      ct: ClassTag[Array[I]],
      ict: ClassTag[I]
  ): Function1[List[I], Array[I]] =
    _.toArray

  private def decodeList[I](result: List[I], bv: ByteVector)(using
      ie: TypeDecoder[I],
      ct: ClassTag[I]
  ): Result[List[I]] =
    decodeCount(bv) >>= {
      case Outcome(0L, bv) =>
        TypeDecoder.Result.success(result, bv)
      case Outcome(count, bv) if count > 0 =>
        decodeBlockItems[I](count, bv).flatMap { case Outcome(curr, restBv) =>
          decodeList[I](result ++ curr, restBv)
        }
      case Outcome(count, bv) =>
        decodeBlock[I](count, bv).flatMap { case Outcome(curr, restBv) =>
          decodeList[I](result ++ curr, restBv)
        }
    }

  private lazy val decodeCount: ByteVector => Result[Long] =
    TypeDecoder[Long].decode

  private def decodeBlockItems[I](count: Long, bv: ByteVector)(using
      ie: TypeDecoder[I],
      ct: ClassTag[I]
  ): Result[List[I]] =
    decodeItems(count, Result.success(List.empty[I], bv))
      .map { case Outcome(l, bv) => Outcome(l.reverse, bv) }

  private def decodeBlock[I](count: Long, bv: ByteVector)(using
      ie: TypeDecoder[I],
      ct: ClassTag[I]
  ): Result[List[I]] =
    for
      bso <- decodeBlockSize(bv)
      res <- decodeBlockItems(count * -1, bso.leftBytes)
    yield res

  private def decodeBlockSize(bv: ByteVector): Result[Long] =
    TypeDecoder[Long].decode(bv).flatMap {
      case Outcome(blockSize, bv) if bv.size < blockSize =>
        Result.failure("Expected array block size < number of available bytes")
      case outcome =>
        Result.success(outcome)
    }

  @tailrec
  private def decodeItems[I](
      leftItems: Long,
      res: Result[List[I]]
  )(using ie: TypeDecoder[I]): Result[List[I]] =
    if res.isLeft then res
    else if leftItems == 0L then res
    else
      val newRes = res.flatMap { case Outcome(list, bv) =>
        ie.decode(bv)
          .map { case Outcome(di, rest) => Outcome(di :: list, rest) }
      }
      decodeItems(leftItems - 1, newRes)
