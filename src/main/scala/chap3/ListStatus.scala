package chap3


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

/**
  * Created by zhoudunxiong on 2018/11/8.
  */
object ListStatus {

  /** listStatus 列出文件或者目录下文件的元数据信息, listStatus并不会递归查找 **/
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val paths = args.map(uri => new Path(uri))
    val status = fs.listStatus(paths)
    val listedPaths = FileUtil.stat2Paths(status)
    listedPaths.foreach(println)
  }
}
