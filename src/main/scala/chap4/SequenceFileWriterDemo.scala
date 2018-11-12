package chap4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IOUtils, IntWritable, SequenceFile, Text}

/**
  * Created by zhoudunxiong on 2018/11/12.
  */
object SequenceFileWriterDemo {

  val DATA = Array("zdx", "ygy", "zdx and ygy")

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(uri)
    val key = new IntWritable()
    val value = new Text()
    var writer: SequenceFile.Writer = null
    try {
      writer = SequenceFile.createWriter(fs, conf, path, key.getClass, value.getClass)
      (0 until 100).foreach { i =>
        key.set(100 - i)
        value.set(DATA(i % DATA.length))
        printf("[%s]\t%s\t%s\n", writer.getLength, key, value)
        writer.append(key, value)
      }
    } finally {
      IOUtils.closeStream(writer)
    }
  }
}
