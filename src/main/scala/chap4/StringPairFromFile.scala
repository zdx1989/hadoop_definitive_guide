package chap4

import java.io.File

import org.apache.avro.Schema.Parser
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.util.Utf8

/**
  * Created by zhoudunxiong on 2018/11/11.
  */
object StringPairFromFile {

  def main(args: Array[String]): Unit = {
    val parser = new Parser()
    val schema = parser.parse(getClass.getResourceAsStream("/StringPair.avsc"))
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    val file = new File("data.avro")
    dataFileWriter.create(schema, file)
    val datum = new Record(schema)
    datum.put("left", new Utf8("L"))
    datum.put("right", new Utf8("R"))
    dataFileWriter.append(datum)
    dataFileWriter.flush()
    dataFileWriter.close()

    val reader = new GenericDatumReader[GenericRecord]()
    val dataFileReader = new DataFileReader[GenericRecord](file, reader)
    assert(dataFileReader.getSchema == schema)

    assert(dataFileReader.hasNext)
    val result = dataFileReader.next()
    assert(result.get("left").toString == "L")
    assert(result.get("right").toString == "R")
    assert(!dataFileReader.hasNext)
  }
}
