package chap4

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodecFactory



/**
  * Created by zhoudunxiong on 2018/11/10.
  */
object FileDecompressor {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val factory = new CompressionCodecFactory(conf)
    val codec = factory.getCodec(new Path(uri))
    if (codec == null) {
      System.err.println(s"No codec found for $uri")
      System.exit(1)
    }
    val outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension)
    var in: InputStream = null
    var out: OutputStream = null
    try {
      in = codec.createInputStream(fs.open(new Path(uri)))
      out = fs.create(new Path(outputUri))
      IOUtils.copyBytes(in, out, conf)
    } finally {
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
    }
  }
}
