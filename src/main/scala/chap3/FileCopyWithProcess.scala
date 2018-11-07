package chap3

import java.io.{BufferedInputStream, FileInputStream, InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.util.Progressable

/**
  * Created by zhoudunxiong on 2018/11/7.
  */
object FileCopyWithProcess {

  def main(args: Array[String]): Unit = {
    val localSrc = args(0)
    val dst = args(1)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val in = new BufferedInputStream(new FileInputStream(localSrc))
    /** 每次将64KB数据包写入datanode管线后触发process方法调用，不过为什么是64KB？**/
    val out = fs.create(new Path(dst), new Progressable() {
      override def progress(): Unit = {
        System.out.print(".")
      }
    })
    IOUtils.copyBytes(in, out, 4096, true)
  }
}
