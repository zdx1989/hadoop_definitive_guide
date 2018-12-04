package chap14

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.apache.zookeeper.{CreateMode, KeeperException, Watcher}
import org.apache.zookeeper.ZooDefs.Ids

/**
  * Created by zhoudunxiong on 2018/11/28.
  */
class ActiveKeyValueStore extends ConnectionWatcher {

  val CHARSET: Charset = Charset.forName("UTF-8")

  val MAX_TRIES = 5

  val RETRY_PERIOD_SECOND = 2

  def write(path: String, value: String): Unit = {
    var retires = 0
    try {
      val stat = zk.exists(path, false)
      if (stat == null)
        zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      else
        zk.setData(path, value.getBytes(CHARSET), -1)
    } catch {
      case e: KeeperException.SessionExpiredException => throw e
      case e: KeeperException =>
        retires += 1
        if (retires == MAX_TRIES) throw e
        TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECOND)

    }
  }

  def read(path: String, watcher: Watcher): String = {
    val data = zk.getData(path, watcher, null)
    new String(data, CHARSET)
  }
}
