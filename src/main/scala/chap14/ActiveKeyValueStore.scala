package chap14

import java.nio.charset.Charset

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids

/**
  * Created by zhoudunxiong on 2018/11/28.
  */
class ActiveKeyValueStore extends ConnectionWatcher {

  val CHARSET: Charset = Charset.forName("UTF-8")

  def write(path: String, value: String): Unit = {
    val stat = zk.exists(path, false)
    if (stat == null) zk.create(path, value.getBytes(CHARSET),
      Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    else
      zk.setData(path, value.getBytes(CHARSET), -1)
  }
}
