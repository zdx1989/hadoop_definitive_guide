package chap4

import org.apache.hadoop.io.Text

/**
  * Created by zhoudunxiong on 2018/11/10.
  */
object TextTest {

  def main(args: Array[String]): Unit = {
    val t = new Text("hadoop")
    assert(t.getLength == 6)
    assert(t.getBytes.length == 6)
    assert(t.charAt(2) == 'd'.toInt)
    assert(t.charAt(100) == -1, "Out of bounds")

    assert(t.find("do") == 2, "Find a substring")
    assert(t.find("o") == 3, "Find first 'o'" )
    assert(t.find("o", 4) == 4, "Find 'o' from position 4 or later")
    assert(t.find("pig") == -1, "Not match")

    t.set("pig")
    assert(t.getLength == 3)
    assert(t.getBytes.length == 3)
    assert(new Text("hadoop").toString == "hadoop")
  }
}
