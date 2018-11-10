package chap4

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.hadoop.io.{IntWritable, VIntWritable, Writable, WritableComparator}
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

    val comparator = WritableComparator.get(classOf[IntWritable])
    val w1 = new IntWritable(163)
    val w2 = new IntWritable(7)
    assert(comparator.compare(w1, w2) > 0)

    val data = serialize(new VIntWritable(163))
    assert(StringUtils.byteToHexString(data) == "8fa3")
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
