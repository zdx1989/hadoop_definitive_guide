package chap4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, Compressor}
import org.apache.hadoop.util.ReflectionUtils

/**
  * Created by zhoudunxiong on 2018/11/10.
  */
object PooledStreamCompressor {


  /** Compressor 根本就没有起作用，从CodecPool中get，没做任何操作又return ？？？**/
  def main(args: Array[String]): Unit = {
    val codecClassName = args(0)
    val codecClass = Class.forName(codecClassName)
    val conf = new Configuration()
    val codec = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
    var compressor: Compressor = null
    try {
      compressor = CodecPool.getCompressor(codec)
      val out = codec.createOutputStream(System.out)
      IOUtils.copyBytes(System.in, out, 4096, false)
      out.flush()
    } finally {
      CodecPool.returnCompressor(compressor)
    }
  }
}
