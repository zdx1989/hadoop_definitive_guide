package chap4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{MapFile, SequenceFile, Writable}

/**
  * Created by zhoudunxiong on 2018/11/13.
  */
object MapFileFixer {

  /** fix方法可以帮助修复MapFile的索引文件，或者为排序好的SequenceFile添加索引文件**/
  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val map = new Path(uri)
    val mapData = new Path(map, MapFile.DATA_FILE_NAME)
    val reader = new SequenceFile.Reader(fs, mapData, conf)
    val keyClass = reader.getKeyClass.asSubclass(classOf[Writable])
    val valueClass = reader.getValueClass.asSubclass(classOf[Writable])
    reader.close()
    val entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf)
    printf("Create MapFile %s with %d entries\n", map, entries)
  }
}
