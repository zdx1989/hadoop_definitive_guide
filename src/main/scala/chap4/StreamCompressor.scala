package chap4

import java.io.ByteArrayInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.util.ReflectionUtils


/**
  * Created by zhoudunxiong on 2018/11/9.
  */
object StreamCompressor {

  def main(args: Array[String]): Unit = {
    val codecClassName = args(0)
    val codecClass = Class.forName(codecClassName)
    val conf = new Configuration()
    val codec = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
    val in = new ByteArrayInputStream("zdx".getBytes())
    val out = codec.createOutputStream(System.out)
    //IOUtils.copyBytes(System.in, out, 4096, false)
    IOUtils.copyBytes(in, out, 4096, false)
    out.flush()
  }
}
