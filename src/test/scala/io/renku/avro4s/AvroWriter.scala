package io.renku.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{Encoder, EncoderFactory}
import org.apache.avro.util.ByteBufferOutputStream
import scodec.bits.ByteVector

import java.io.OutputStream
import scala.jdk.CollectionConverters.*

private trait AvroWriter:
  def write[A](values: Seq[A], encoder: A => Any): ByteVector

private object AvroWriter:
  val blockSize: Int = 64 // in Bytes; this is the min block size as in org.apache.avro.io.EncoderFactory.MIN_BLOCK_BUFFER_SIZE
  private val nonBlockingEncoder: OutputStream => Encoder =
    EncoderFactory.get().binaryEncoder(_, null)
  private val blockingEncoder: OutputStream => Encoder =
    new EncoderFactory().configureBlockSize(blockSize).blockingBinaryEncoder(_, null)

  def apply(schema: Schema): AvroWriter =
    new Impl(schema, nonBlockingEncoder)

  def blocking(schema: Schema): AvroWriter =
    new Impl(schema, blockingEncoder)

  private class Impl(
      schema: Schema,
      encoderFactory: OutputStream => Encoder
  ) extends AvroWriter:
    private[this] val writer = new GenericDatumWriter[Any](schema)

    def write[A](values: Seq[A], encoder: A => Any): ByteVector =
      write0(values, encoder)

    private def write0[A](
        values: Seq[A],
        encoder: A => Any
    ): ByteVector = {
      val baos = new ByteBufferOutputStream()
      val ef = encoderFactory(baos)
      values.map(encoder).foreach(writer.write(_, ef))
      ef.flush()

      baos.getBufferList.asScala
        .map(ByteVector.view)
        .foldLeft(ByteVector.empty)(_ ++ _)
    }
