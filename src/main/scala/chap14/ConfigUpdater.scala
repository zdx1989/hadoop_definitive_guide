package chap14

import java.util.Random
import java.util.concurrent.TimeUnit

import org.apache.zookeeper.KeeperException

/**
  * Created by zhoudunxiong on 2018/11/28.
  */
class ConfigUpdater(store: ActiveKeyValueStore) {
  import ConfigUpdater._

  private val random: Random = new Random()


  def this(host: String) = {
    this(new ActiveKeyValueStore)
    store.connect(host)
  }

  def run(): Unit = {
    while (true) {
      val value = random.nextInt(100) + ""
      store.write(PATH, value)
      println(s"Set $PATH to $value")
      TimeUnit.SECONDS.sleep(random.nextInt(10))
    }
  }
}

object ConfigUpdater {

  val PATH: String = "/config"

  def main(args: Array[String]): Unit = {
    try {
      val configUpdater = new ConfigUpdater(args(0))
      configUpdater.run()
    } catch {
      case _: KeeperException.SessionExpiredException =>
        // start a new session
      case e: KeeperException =>
        // already retried, so exit
      e.printStackTrace()
    }


  }
}
