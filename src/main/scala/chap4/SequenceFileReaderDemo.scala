package chap4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.util.ReflectionUtils

/**
  * Created by zhoudunxiong on 2018/11/12.
  */
object SequenceFileReaderDemo {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(uri)
    var reader: SequenceFile.Reader = null
    try {
      reader = new SequenceFile.Reader(fs, path, conf)
      val key = ReflectionUtils.newInstance(reader.getKeyClass, conf).asInstanceOf[Writable]
      val value = ReflectionUtils.newInstance(reader.getValueClass, conf).asInstanceOf[Writable]
      var position = reader.getPosition
      while (reader.next(key, value)) {
        val syncSeen = if (reader.syncSeen()) "*" else ""
        printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value)
        position = reader.getPosition

      }

      reader.seek(256)
      assert(reader.next(key, value))
      assert(key.asInstanceOf[IntWritable].get() == 95)
      assert(value.asInstanceOf[Text].toString == "zdx and ygy")
      reader.seek(360)
      //assert(reader.next(key, value))
    } finally {
      IOUtils.closeStream(reader)
    }

  }
}
