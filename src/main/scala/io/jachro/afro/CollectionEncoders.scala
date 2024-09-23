package io.jachro.afro

import cats.syntax.all.*
import io.jachro.afro.TypeEncoder.Result
import scodec.bits.ByteVector

import scala.annotation.targetName
import scala.reflect.ClassTag

trait CollectionEncoders extends PrimitiveTypeEncoders:

  val defaultBinaryBlockSize: Int = 64 * 1024
  val minBinaryBlockSize: Int = 64
  val maxBinaryBlockSize: Int = 1024 * 1024 * 1024

  given arrayTypeEncoder[I](using ie: TypeEncoder[I]): TypeEncoder[Array[I]] =
    nonBlockingArrayEncoder

  given seqTypeEncoder[C[X] <: collection.Seq[X], I](using
      ie: TypeEncoder[I]
  ): TypeEncoder[C[I]] =
    nonBlockingSeqEncoder

  given setTypeEncoder[C[X] <: collection.Set[X], I](using
      ie: TypeEncoder[I]
  ): TypeEncoder[C[I]] =
    nonBlockingSetEncoder

  def nonBlockingArrayEncoder[I](using
      ie: TypeEncoder[I]
  ): TypeEncoder[Array[I]] =
    TypeEncoder
      .instance[List[I]](encodeList[I])
      .contramap[Array[I]](Converters.arrayToList)

  def nonBlockingSeqEncoder[C[X] <: collection.Seq[X], I](using
      ie: TypeEncoder[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder
      .instance[List[I]](encodeList[I])
      .contramap[C[I]](Converters.seqToList)

  def nonBlockingSetEncoder[C[X] <: collection.Set[X], I](using
      ie: TypeEncoder[I]
  ): TypeEncoder[C[I]] =
    TypeEncoder
      .instance[List[I]](encodeList[I])
      .contramap[C[I]](Converters.setToList)

  def blockingArrayEncoder[I](blockSize: Int = defaultBinaryBlockSize)(using
      ie: TypeEncoder[I]
  ): TypeEncoder[Array[I]] =
    TypeEncoder
      .instance[List[I]](encodeBlockingList[I](blockSize))
      .contramap(Converters.arrayToList)

  def blockingSeqEncoder[C[X] <: collection.Seq[X], I](
      blockSize: Int = defaultBinaryBlockSize
  )(using ie: TypeEncoder[I]): TypeEncoder[C[I]] =
    TypeEncoder
      .instance[List[I]](encodeBlockingList[I](blockSize))
      .contramap(Converters.seqToList)

  def blockingSetEncoder[C[X] <: collection.Set[X], I](
      blockSize: Int = defaultBinaryBlockSize
  )(using ie: TypeEncoder[I]): TypeEncoder[C[I]] =
    TypeEncoder
      .instance[List[I]](encodeBlockingList[I](blockSize))
      .contramap(Converters.setToList)

  protected def encodeList[I](using ie: TypeEncoder[I]): List[I] => Result = {
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

  private object Converters:
    def arrayToList[I]: Array[I] => List[I] = _.toList
    def seqToList[I]: collection.Seq[I] => List[I] = _.toList
    def setToList[I]: collection.Set[I] => List[I] = _.toList

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

  protected def encodeBlockingList[I](blockSize: Int)(using
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
