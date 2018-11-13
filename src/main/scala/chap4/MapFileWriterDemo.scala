package chap4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{IOUtils, IntWritable, MapFile, Text}

/**
  * Created by zhoudunxiong on 2018/11/13.
  */
object MapFileWriterDemo {

  val DATA: Array[String] = Array(
    "zdx",
    "ygy",
    "zdx and ygy"
  )

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val key = new IntWritable()
    val value = new Text()
    var writer: MapFile.Writer = null
    try {
      writer = new MapFile.Writer(conf, fs, uri, key.getClass, value.getClass)
      (0 until 1024).foreach { i =>
        key.set(i + 1)
        value.set(DATA(i % DATA.length))
        writer.append(key, value)
      }
    } finally {
      IOUtils.closeStream(writer)
    }
  }
}
