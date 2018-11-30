package chap14

import java.util.Random
import java.util.concurrent.TimeUnit

/**
  * Created by zhoudunxiong on 2018/11/28.
  */
class ConfigUpdater {

  private val PATH: String = "/config"

  private var store: ActiveKeyValueStore = _

  private val random: Random = new Random()

  def ConfigUpdate(host: String): Unit = {
    store = new ActiveKeyValueStore()
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

  def main(args: Array[String]): Unit = {
    val configUpdater = new ConfigUpdater()
    configUpdater.run()

  }
}
