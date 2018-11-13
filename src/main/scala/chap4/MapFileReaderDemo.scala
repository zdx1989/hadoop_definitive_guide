package chap4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{IOUtils, IntWritable, MapFile, Text}

/**
  * Created by zhoudunxiong on 2018/11/13.
  */
object MapFileReaderDemo {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    var reader: MapFile.Reader = null
    try {
      reader = new MapFile.Reader(fs, uri, conf)
      val value = new Text()
      reader.get(new IntWritable(496), value)
      assert(value.toString == "zdx")
    } finally {
      IOUtils.closeStream(reader)
    }
  }
}
