package io.jachro.afro

import org.apache.avro.Schema as AvroSchema
import org.apache.avro.Schema.Parser as AvroParser
import org.apache.avro.generic.GenericRecord

final private case class OfficialAvroLibRecord(schema: String, values: Seq[Any])
    extends GenericRecord:

  override def put(key: String, v: Any): Unit = sys.error("immutable record")

  override def put(i: Int, v: Any): Unit = sys.error("Immutable record")

  override def get(key: String): Any = {
    val field = getSchema.getField(key)
    if (field == null)
      sys.error(s"Field $key does not exist in record schema=$schema, values=$values")
    else values(field.pos())
  }

  override def get(i: Int): Any = values(i)

  override lazy val getSchema: AvroSchema = new AvroParser().parse(schema)
