package chap14

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by zhoudunxiong on 2018/11/26.
  */
class ConnectionWatcher extends Watcher {

  private val SESSION_TIMEOUT = 5000

  protected var zk: ZooKeeper = _

  private val connectSignal = new CountDownLatch(1)

  def connect(hosts: String): Unit = {
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this)
    connectSignal.await()
  }

  override def process(event: WatchedEvent): Unit = {
    if (event.getState == KeeperState.SyncConnected)
      connectSignal.countDown()
  }

  def close(): Unit = {
    zk.close()
  }
}
