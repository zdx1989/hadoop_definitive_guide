package chap4

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.hadoop.io.{IntWritable, Writable}
import org.apache.hadoop.util.StringUtils

/**
  * Created by zhoudunxiong on 2018/11/10.
  */
object IntWritableTest {

  def main(args: Array[String]): Unit = {
    val writable = new IntWritable(163)
    val bytes = serialize(writable)
    /** 一个整数占用4个字节 **/
    assert(bytes.length == 4)
    assert(StringUtils.byteToHexString(bytes) == "000000a3")

    val newWritable = new IntWritable()
    deserialize(newWritable, bytes)
    assert(writable.get() == 163)
  }

  def serialize(writable: Writable): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(out)
    writable.write(dataOut)
    dataOut.close()
    out.toByteArray
  }

  def deserialize(writable: Writable, bytes: Array[Byte]): Array[Byte] = {
    val in = new ByteArrayInputStream(bytes)
    val dataIn = new DataInputStream(in)
    writable.readFields(dataIn)
    dataIn.close()
    bytes
  }

}
