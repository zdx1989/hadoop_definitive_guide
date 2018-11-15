package chap5

import org.apache.hadoop.conf.Configuration

/**
  * Created by zhoudunxiong on 2018/11/15.
  */
object ConfigurationTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.addResource("configuration-1.xml")
    assert(conf.get("color") == "yellow")
    assert(conf.getInt("size", 0) == 10)
    assert(conf.get("breadth", "size") == "size")

    conf.addResource("configuration-2.xml")
    assert(conf.getInt("size", 0) == 12)
    assert(conf.get("weight") == "heavy")

    assert(conf.get("size-weight") == "12,heavy")
    System.setProperty("size", "14")
    assert(conf.get("size-weight") == "14,heavy")
    System.setProperty("color", "red")
    assert(conf.get("color") == "yellow")
    System.setProperty("length", "2")
    assert(conf.get("length") == null)
  }
}
