package io.renku.avro4s

import cats.syntax.all.*
import io.renku.avro4s.TypeEncoder.TypeEncodingResult
import scodec.bits.ByteVector

import scala.annotation.targetName
import scala.reflect.ClassTag

trait CollectionEncoders extends PrimitiveTypeEncoders:

  val defaultBinaryBlockSize: Int = 64 * 1024
  val minBinaryBlockSize: Int = 64
  val maxBinaryBlockSize: Int = 1024 * 1024 * 1024

  given [C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => Array[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder.instance[Array[I]](encodeArray[I]).contramap[C[I]](converter)

  given [I](using ct: ClassTag[Set[I]], ict: ClassTag[I]): Function1[Set[I], Array[I]] =
    _.toArray
  given [I](using ct: ClassTag[List[I]], ict: ClassTag[I]): Function1[List[I], Array[I]] =
    _.toArray

  def blockingArrayEncoder[C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => Array[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] = blockingArrayEncoder(defaultBinaryBlockSize)

  def blockingArrayEncoder[C[X], I](blockSize: Int)(using
      ie: TypeEncoder[I],
      converter: C[I] => Array[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder
      .instance[Array[I]](encodeBlockingArray[I](blockSize))
      .contramap(converter)

  private def encodeArray[I](using
      ie: TypeEncoder[I],
      ct: ClassTag[Array[I]]
  ): Array[I] => TypeEncodingResult = {
    case arr if arr.isEmpty =>
      TypeEncoder[Long].encodeValue(0L)
    case arr =>
      for
        encLength <- TypeEncoder[Long].encodeValue(arr.length)
        encItems <- arr.foldLeft(TypeEncodingResult.success(encLength)) {
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

  private def encodeBlockingArray[I](blockSize: Int)(using
      ie: TypeEncoder[I],
      ct: ClassTag[Array[I]]
  ): Array[I] => TypeEncodingResult = {
    case arr if arr.isEmpty =>
      TypeEncoder[Long].encodeValue(0L)
    case arr =>
      for
        encItems <- arr.toList.map(ie.encodeValue).sequence
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
