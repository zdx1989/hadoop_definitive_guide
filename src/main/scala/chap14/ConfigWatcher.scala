package chap14

import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher}

/**
  * Created by zhoudunxiong on 2018/12/1.
  */
class ConfigWatcher extends Watcher {

  private var store: ActiveKeyValueStore = _

  def this(host: String) = {
    this
    store = new ActiveKeyValueStore()
    store.connect(host)
  }

  def displayConfig(): Unit = {
    val value = store.read(ConfigUpdater.PATH, this)
    println(s"Read ${ConfigUpdater.PATH} $value")
  }

  override def process(event: WatchedEvent): Unit = {
    if (event.getType == EventType.NodeDataChanged) {
      try {
        displayConfig()
      } catch {
        case _: InterruptedException =>
          System.err.println("Interrupted. Exiting.")
          Thread.currentThread().interrupt()
        case e: KeeperException =>
          System.err.println(s"KeeperException: $e. Exiting")
      }
    }
  }
}

object ConfigWatcher {

  def main(args: Array[String]): Unit = {
    val configWatcher = new ConfigWatcher(args(0))
    configWatcher.displayConfig()
    Thread.sleep(Long.MaxValue)
  }

}
