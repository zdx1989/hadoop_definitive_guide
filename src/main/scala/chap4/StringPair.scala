package chap4

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8

/**
  * Created by zhoudunxiong on 2018/11/11.
  */
object StringPair {

  def main(args: Array[String]): Unit = {
    val parser = new Parser()
    val schema = parser.parse(this.getClass.getResourceAsStream("/StringPair.avsc"))
    val datum = new Record(schema)
    datum.put("left", new Utf8("L"))
    datum.put("right", new Utf8("R"))
    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(datum, encoder)
    encoder.flush()
    out.close()

    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(out.toByteArray, null)
    val result = reader.read(null, decoder)
    assert(result.get("left").toString == "L")
    assert(result.get("right").toString == "R")
  }
}
