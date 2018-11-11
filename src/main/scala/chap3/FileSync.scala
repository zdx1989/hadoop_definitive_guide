package chap3


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by zhoudunxiong on 2018/11/9.
  */
object FileSync {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val p = new Path(uri)
    val out = fs.create(p)
    assert(fs.exists(p))

    out.write("content\n".getBytes("UTF-8"))
    out.flush()
    assert(fs.getFileStatus(p).getLen == 0L)

    out.hsync()
    //out.close()
    assert(fs.getFileStatus(p).getLen > 0L)

  }
}
