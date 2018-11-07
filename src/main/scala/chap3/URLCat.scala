package chap3

import java.io.InputStream
import java.net.URL

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.hadoop.io.IOUtils


/**
  * Created by zhoudunxiong on 2018/11/7.
  */
object URLCat {

  /**
    * 一个JVM只能调用一次setURLStreamHandlerFactory方法，所以假如已有第三方设置该方法
    * 则无法使用该方法从Hadoop中读取数据
    */
  URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())

  def main(args: Array[String]): Unit = {
    var in: InputStream = null
    try {
      in = new URL(args(0)).openStream()
      IOUtils.copyBytes(in, System.out, 4096, false)
    } finally {
      IOUtils.closeStream(in)
    }
  }
}
