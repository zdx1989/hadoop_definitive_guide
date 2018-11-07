package chap3

import java.io.InputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils


/**
  * Created by zhoudunxiong on 2018/11/7.
  */
object FileSystemCat {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val key = args(1)
    val conf = new Configuration()
    if (key == "uri") getByUri(uri, conf)
    else getByConf(uri, conf)
  }

  /**
    * 根据URI来读取文件系统的数据，读取本地文件还是HDFS中的文件或其它文件系统的文件由URI来决定
    * @param uri hdfs://localhost:9000/test/README.md, file:///test/README.md
    * @param conf
    */
  def getByUri(uri: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(URI.create(uri), conf)
    get(uri, fs)
  }

  /**
    * 根据Configuration来读取文件系统的数据，读取本地文件还是HDFS中的文件或其它文件系统的文件由
    * classpath中core-site.xml文件中的fs.default.name属性决定
    * @param uri 对于HDFS，可以省略主机和端口直接使用绝对路径: /test/README.md
    * @param conf
    */
  def getByConf(uri: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(conf)
    get(uri, fs)
  }

  def get(uri: String, fs: FileSystem): Unit = {
    var in: InputStream = null
    try {
      in = fs.open(new Path(uri))
      IOUtils.copyBytes(in, System.out, 4096, false)
    } finally {
      IOUtils.closeStream(in)
    }
  }

}
