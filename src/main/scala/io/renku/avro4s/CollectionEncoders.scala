package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.TypeEncoder.Result
import scodec.bits.ByteVector

import scala.annotation.targetName
import scala.reflect.ClassTag

trait CollectionEncoders extends PrimitiveTypeEncoders:

  val defaultBinaryBlockSize: Int = 64 * 1024
  val minBinaryBlockSize: Int = 64
  val maxBinaryBlockSize: Int = 1024 * 1024 * 1024

  given [C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => List[I]
  ): TypeEncoder[C[I]] =
    nonBlockingListEncoder[C, I]

  def nonBlockingListEncoder[C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => List[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder.instance[List[I]](encodeList[I]).contramap[C[I]](converter)

  def blockingListEncoder[C[X], I](blockSize: Int)(using
      ie: TypeEncoder[I],
      converter: C[I] => List[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder
      .instance[List[I]](encodeBlockingList[I](blockSize))
      .contramap(converter)

  def blockingListEncoder[C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => List[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] = blockingListEncoder(defaultBinaryBlockSize)

  /** This encoder tries to choose the best encoding strategy for the given collection of
    * items. By checking the size of first item encoded to bytes multiplied by the number
    * of items is:
    *   - < than 75% of the `minBinaryBlockSize` the non-blocking encoder is used
    *   - < than 75% of the `defaultBinaryBlockSize` and the item size is <
    *     `minBinaryBlockSize` is the blocking encoder with `minBinaryBlockSize` is used
    *   - < than 75% of the `maxBinaryBlockSize` and the item size is <
    *     `defaultBinaryBlockSize` the blocking encoder with `defaultBinaryBlockSize` is
    *     used
    * In any other case, the blocking encoder with `maxBinaryBlockSize` is used.
    *
    * @param ie
    *   TypeEncoder of the collection items
    * @param converter
    *   a function transforming the given collection to List
    * @param ict
    *   ClassTag of the collection item type
    * @tparam C
    *   type of the collection
    * @tparam I
    *   type of the item
    * @return
    *   TypeEncoder.Result
    */
  def autoListEncoder[C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => List[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder.instance[List[I]] {
      case l @ Nil =>
        nonBlockingListEncoder[C, I].encodeValue(l.asInstanceOf[C[I]])
      case l @ h :: tail =>
        TypeEncoder[I].encodeValue(h).map(_.size) >>= { size =>
          if size * l.length < .75 * minBinaryBlockSize then
            nonBlockingListEncoder[C, I].encodeValue(l.asInstanceOf[C[I]])
          else if size * l.length < .75 * defaultBinaryBlockSize && size < minBinaryBlockSize
          then
            blockingListEncoder[C, I](minBinaryBlockSize)
              .encodeValue(l.asInstanceOf[C[I]])
          else if size * l.length < .75 * maxBinaryBlockSize && size < defaultBinaryBlockSize
          then
            blockingListEncoder[C, I](defaultBinaryBlockSize)
              .encodeValue(l.asInstanceOf[C[I]])
          else
            blockingListEncoder[C, I](maxBinaryBlockSize)
              .encodeValue(l.asInstanceOf[C[I]])
        }
    }
    TypeEncoder.instance[List[I]](encodeList[I]).contramap[C[I]](converter)

  given [I](using
      ct: ClassTag[Array[I]],
      ict: ClassTag[I]
  ): Function1[Array[I], List[I]] =
    _.toList
  given [C[X] <: IterableOnce[X], I](using
      ct: ClassTag[C[I]],
      ict: ClassTag[I]
  ): Function1[C[I], List[I]] =
    _.iterator.toList

  private def encodeList[I](using ie: TypeEncoder[I]): List[I] => Result = {
    case Nil =>
      TypeEncoder[Long].encodeValue(0L)
    case list =>
      for
        encLength <- TypeEncoder[Long].encodeValue(list.length)
        encItems <- list.foldLeft(TypeEncoder.Result.success(encLength)) {
          case (enc: Left[_, _], _) => enc
          case (Right(bv), i)       => ie.encodeValue(i).map(bv ++ _)
        }
      yield encItems :+ 0.toByte
  }

  private case class EncodingBuffer(items: Int, encoded: ByteVector):
    lazy val size: Long = encoded.size

    @targetName("append")
    def :+(b: EncodingBuffer): EncodingBuffer =
      copy(items = items + b.items, encoded ++ b.encoded)

  private object EncodingBuffer:
    val empty: EncodingBuffer = EncodingBuffer(0, ByteVector.empty)
    def ofOneItem(bv: ByteVector): EncodingBuffer = EncodingBuffer(1, bv)
    given TypeEncoder[EncodingBuffer] = (eb: EncodingBuffer) =>
      for
        items <- TypeEncoder[Long].encodeValue(eb.items.toLong * -1)
        size <- TypeEncoder[Long].encodeValue(eb.size)
      yield items ++ size ++ eb.encoded

  private def encodeBlockingList[I](blockSize: Int)(using
      ie: TypeEncoder[I],
      ct: ClassTag[List[I]]
  ): List[I] => Result = {
    case Nil =>
      TypeEncoder[Long].encodeValue(0L)
    case list =>
      for
        encItems <- list.map(ie.encodeValue).sequence
        buff <- encItems.map(EncodingBuffer.ofOneItem).asRight[AvroEncodingException]
        blocks <- buff.tail.foldLeft(List(buff.head))(formBlocks(blockSize)).asRight
        encBlocks <- blocks.map(TypeEncoder[EncodingBuffer].encodeValue).sequence
      yield encBlocks.reduce(_ ++ _) :+ 0.toByte
  }

  private def formBlocks(
      blockSize: Int
  ): (List[EncodingBuffer], EncodingBuffer) => List[EncodingBuffer] = {
    case (Nil, curr) => curr :: Nil
    // there's still enough space for one item more in the block
    case (acc @ accHd :: accTl, curr) if acc.size + curr.size < blockSize =>
      (accHd :+ curr) :: accTl
    case (acc @ accHd :: accTl, curr) =>
      curr :: acc
  }
