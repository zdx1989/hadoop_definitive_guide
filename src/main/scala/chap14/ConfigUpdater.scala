package chap14

import java.util.Random

/**
  * Created by zhoudunxiong on 2018/11/28.
  */
class ConfigUpdater {

  private val PATH: String = "/config"

  private var store: ActiveKeyValueStore = _

  private val random: Random = new Random()

  def ConfigUpdate(host: String): Unit = ???
}
