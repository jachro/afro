package io.renku.avro4s

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{Encoder, EncoderFactory}
import org.apache.avro.util.ByteBufferOutputStream
import scodec.bits.ByteVector

import java.io.OutputStream
import scala.jdk.CollectionConverters.*

trait AvroWriter:
  def write[A](values: Seq[A], encoder: A => Any): ByteVector

object AvroWriter:
  def apply(
      schema: Schema,
      codecFactory: CodecFactory = CodecFactory.nullCodec()
  ): AvroWriter =
    new Impl(schema, codecFactory)

  private class Impl(schema: Schema, cf: CodecFactory) extends AvroWriter:
    private[this] val writer = new GenericDatumWriter[Any](schema)

    def write[A](values: Seq[A], encoder: A => Any): ByteVector =
      write0(out => EncoderFactory.get().binaryEncoder(out, null), values, encoder)

    private def write0[A](
        makeEncoder: OutputStream => Encoder,
        values: Seq[A],
        encoder: A => Any
    ): ByteVector = {
      val baos = new ByteBufferOutputStream()
      val ef = makeEncoder(baos)
      values.map(encoder).foreach(writer.write(_, ef))
      ef.flush()

      baos.getBufferList.asScala
        .map(ByteVector.view)
        .foldLeft(ByteVector.empty)(_ ++ _)
    }
