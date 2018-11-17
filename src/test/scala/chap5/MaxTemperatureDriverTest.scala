package chap5

import chap5.v1.MaxTemperatureDriver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.Test
import org.hamcrest.MatcherAssert._
import org.hamcrest.CoreMatchers._

/**
  * Created by zhoudunxiong on 2018/11/17.
  */
class MaxTemperatureDriverTest {

  @Test
  def driverTest(): Unit = {
    val conf = new Configuration()
    conf.set("fs.default.name", "file:///")
    conf.set("mapred.job.tracker", "local")
    val input = new Path("/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/ncdc/micro")
    val output = new Path("out")

    val fs = FileSystem.getLocal(conf)
    fs.delete(output, true)

    val driver = new MaxTemperatureDriver();
    driver.setConf(conf)

    val exitCode = driver.run(Array(input.toString, output.toString))
    assert(exitCode == 0)
    assertThat(exitCode, is(0))
  }
}
