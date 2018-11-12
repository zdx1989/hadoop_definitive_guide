package chap4

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8

/**
  * Created by zhoudunxiong on 2018/11/12.
  */
object StringPairNewTest {

  /** 旧的schema写入数据，新的Schema读出新的数据，新增的字段通过默认值读取 **/
  def main(args: Array[String]): Unit = {
    val parser = new Parser()
    val schema = parser.parse(getClass.getResourceAsStream("/StringPair.avsc"))
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val datum = new Record(schema)
    datum.put("left", new Utf8("L"))
    datum.put("right", new Utf8("R"))
    writer.write(datum, encoder)
    encoder.flush()
    out.close()

    val newSchema = parser.parse(getClass.getResourceAsStream("/StringPairNew.avsc"))
    val reader = new GenericDatumReader[GenericRecord](schema, newSchema)
    val decoder = DecoderFactory.get().binaryDecoder(out.toByteArray, null)
    assert(reader.getSchema == schema)
    val result = reader.read(null, decoder)
    assert(result.get("left").toString == "L")
    assert(result.get("right").toString == "R")
    assert(result.get("description").toString == "")
  }
}
