package io.renku.avro4s

import cats.syntax.all.*

import scala.annotation.tailrec
import scala.reflect.ClassTag

trait CollectionDecoders extends PrimitiveTypeDecoders:

  given [I](using ie: TypeDecoder[I], ct: ClassTag[I]): TypeDecoder[Array[I]] =
    TypeDecoder.instance[Array[I]] { bv =>
      TypeDecoder[Long].decode(bv) >>= {
        case (0L, bv) => (Array.empty[I] -> bv).asRight[AvroDecodingException]
        case (length, bv) =>
          decodeItems(length, (List.empty[I] -> bv).asRight)
            .map { case (l, bv) => l.reverse.toArray -> bv }
            .map { case (l, bv) => l -> bv.drop(1) }
      }
    }

  @tailrec
  private def decodeItems[I](
      leftItems: Long,
      res: TypeDecoder.TypeDecodingResult[List[I]]
  )(using ie: TypeDecoder[I]): TypeDecoder.TypeDecodingResult[List[I]] =
    if res.isLeft then res
    else if leftItems == 0L then res
    else
      val newRes = res.flatMap { case (list, bv) =>
        ie.decode(bv).map { case (di, rest) =>
          (di :: list) -> rest
        }
      }
      decodeItems(leftItems - 1, newRes)
