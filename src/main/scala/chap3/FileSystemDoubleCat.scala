package chap3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
  * Created by zhoudunxiong on 2018/11/7.
  */
object FileSystemDoubleCat {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    var in: FSDataInputStream = null
    try {
      in = fs.open(new Path(uri))
      IOUtils.copyBytes(in, System.out, 4096, false)
      in.seek(0)
      IOUtils.copyBytes(in, System.out, 4096, false)
    } finally {
      IOUtils.closeStream(in)
    }
  }
}
