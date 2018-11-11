package chap4

import java.io.ByteArrayOutputStream

import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}


/**
  * Created by zhoudunxiong on 2018/11/11.
  */
object StringPairFromPlugin {

  def main(args: Array[String]): Unit = {
    val datum = new StringPair()
    datum.setLeft("L")
    datum.setRight("R")
    val out = new ByteArrayOutputStream()
    val writer = new SpecificDatumWriter[StringPair](StringPair.getClassSchema)
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(datum, encoder)
    encoder.flush()
    out.close()


    val reader = new SpecificDatumReader[StringPair](StringPair.getClassSchema)
    val decoder = DecoderFactory.get().binaryDecoder(out.toByteArray, null)
    val result = reader.read(null, decoder)
    assert(result.getLeft.toString == "L")
    assert(result.getRight.toString == "R")
  }
}
