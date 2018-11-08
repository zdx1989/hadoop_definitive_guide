package chap3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}

/**
  * Created by zhoudunxiong on 2018/11/8.
  */
class RegexExcludePathFilter(regex: String) extends PathFilter {

  override def accept(path: Path): Boolean = {
    !path.toString.matches(regex)
  }
}

object RegexExcludePathFilter {

  def main(args: Array[String]): Unit = {
    val pathPattern = args(0)
    val regex = args(1)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val status = fs.globStatus(new Path(pathPattern), new RegexExcludePathFilter(regex))
    val listedPath = FileUtil.stat2Paths(status)
    listedPath.foreach(println)
  }
}
