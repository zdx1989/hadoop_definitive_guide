package chap4

import org.apache.avro.Schema.Parser
import org.apache.avro.file.{ DataFileStream, DataFileWriter}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path}

/**
  * Created by zhoudunxiong on 2018/11/12.
  */
object StringPairFromHdfs {

  def main(args: Array[String]): Unit = {
    val parser = new Parser
    val schema = parser.parse(getClass.getResourceAsStream("/StringPair.avsc"))
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)

    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(uri))
    dataFileWriter.create(schema, out)
    val datum = new Record(schema)
    datum.put("left", new Utf8("L"))
    datum.put("right", new Utf8("R"))
    dataFileWriter.append(datum)
    out.hflush()
    dataFileWriter.close()

    val reader = new GenericDatumReader[GenericRecord]()
    val dataFileReader = new DataFileStream[GenericRecord](fs.open(new Path(uri)), reader)
    assert(dataFileReader.getSchema == schema)

    assert(dataFileReader.hasNext)
    assert(dataFileReader.hasNext)
    val result = dataFileReader.next()
    assert(result.get("left").toString == "L")
    assert(result.get("right").toString == "R")
    assert(!dataFileReader.hasNext)
  }
}
