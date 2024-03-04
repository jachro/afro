package io.renku.avro4s

import org.apache.avro.Schema
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.*
import scodec.bits.ByteVector

import java.io.{EOFException, InputStream}

private trait AvroReader:
  def read(input: ByteVector): Seq[Any]

private object AvroReader:
  def apply(schema: Schema): AvroReader = new Impl(schema)

  private class Impl(schema: Schema) extends AvroReader:
    private[this] val reader = new GenericDatumReader[Any](schema)

    extension (self: DatumReader[Any])
      def readOpt(decoder: Decoder): Option[Any] =
        try Option(self.read(null, decoder))
        catch {
          case _: EOFException => None
        }

    override def read(input: ByteVector): Seq[Any] = {
      val in = ByteVectorInput(input)
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      read0(decoder)
    }

    private def read0(
        decoder: BinaryDecoder
    ): Seq[Any] =
      @annotation.tailrec
      def go(r: GenericDatumReader[Any], result: List[Any]): Seq[Any] =
        if (isEnd(decoder)) result.reverse
        else
          r.readOpt(decoder) match
            case None     => result.reverse
            case Some(el) => go(r, el :: result)

      go(reader, Nil)

    private def isEnd(d: JsonDecoder | BinaryDecoder): Boolean = d match
      case jd: JsonDecoder   => false
      case bd: BinaryDecoder => bd.isEnd

final private class ByteVectorInput(val bytes: ByteVector)
    extends InputStream
    with SeekableInput {

  private[this] var position: Long = 0

  override def seek(p: Long): Unit =
    position = p

  override def tell(): Long =
    getPosition

  def getPosition: Long = position

  override val length: Long = bytes.length

  override def read(): Int =
    if (position >= length) -1
    else {
      val b = bytes(position).toInt
      position = position + 1
      b
    }

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    if (len <= 0) 0
    else if (position >= length) -1
    else {
      val until = math.min(position + len, length)
      bytes.slice(position, until).copyToArray(b, off)
      val bytesRead = (until - position).toInt
      position = until
      bytesRead
    }

  override def skip(n: Long): Long = {
    val until = math.min(position + n, length)
    val skipped = until - position
    position = until
    skipped
  }

  override def available(): Int = (length - position).toInt

  override def close(): Unit = ()
}
