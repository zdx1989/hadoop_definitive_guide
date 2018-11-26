package chap14

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids

/**
  * Created by zhoudunxiong on 2018/11/26.
  */
class JoinGroup extends ConnectionWatcher {

  def join(groupName: String, memberName: String): Unit = {
    val path = s"/$groupName/$memberName"
    val createPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    println(s"Created $createPath")
  }

}

object JoinGroup {

  def main(args: Array[String]): Unit = {
    val joinGroup = new JoinGroup()
    joinGroup.connect(args(0))
    joinGroup.join(args(1), args(2))

    Thread.sleep(Long.MaxValue)
  }
}