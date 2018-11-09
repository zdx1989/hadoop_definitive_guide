package chap3

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by zhoudunxiong on 2018/11/9.
  */
object ShowFileStatus {

  def main(args: Array[String]): Unit = {
    val uri = args(0)
    getFileStatus(uri)
  }

  def getFileStatus(uri: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val fileStatus = fs.getFileStatus(new Path(uri))
    println(s"path: ${fileStatus.getPath.toUri.getPath}")
    println(s"isDir: ${fileStatus.isDirectory}")
    println(s"length: ${fileStatus.getLen}")
    println(s"modification time: ${formatTime(fileStatus.getModificationTime)}")
    println(s"replication: ${fileStatus.getReplication}")
    println(s"blockSize: ${fileStatus.getBlockSize}")
    println(s"owner: ${fileStatus.getOwner}")
    println(s"group: ${fileStatus.getGroup}")
    println(s"permission: ${fileStatus.getPermission}")
  }

  def formatTime(time: Long): String = {
    val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    sf.format(new Date(time))
  }
}
