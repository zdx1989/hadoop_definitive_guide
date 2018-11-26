package chap14

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by zhoudunxiong on 2018/11/26.
  */
class CreateGroup extends Watcher {

  private val SESSION_TIMEOUT = 5000

  private val connectSignal = new CountDownLatch(1)

  private var zk: ZooKeeper = _


  def connect(hosts: String): Unit = {
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this)
    connectSignal.await()
  }

  override def process(event: WatchedEvent): Unit = {
    if (event.getState == KeeperState.SyncConnected)
      connectSignal.countDown()
  }

  def create(groupName: String): Unit = {
    val path = "/" + groupName
    val createPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    println(s"Created $createPath")
  }

  def close(): Unit = {
    zk.close()
  }

}

object CreateGroup {

  def main(args: Array[String]): Unit = {
    val createGroup = new CreateGroup()
    createGroup.connect(args(0))
    createGroup.create(args(1))
    createGroup.close()
  }
}
