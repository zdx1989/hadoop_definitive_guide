package chap5

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}

/**
  * Created by zhoudunxiong on 2018/11/15.
  */
class ConfigurationPrinter extends Configured with Tool {

  Configuration.addDefaultResource("hdfs-default.xml")
  Configuration.addDefaultResource("hdfs-site.xml")
  Configuration.addDefaultResource("mapred-default.xml")
  Configuration.addDefaultResource("mapred-site.xml")


  override def run(args: Array[String]): Int = {
    import scala.collection.JavaConversions._
    val conf = getConf
    conf.foreach { entity =>
      printf("%s=%s\n", entity.getKey, entity.getValue)
    }
    return 0
  }

}

object ConfigurationPrinter {

  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(new ConfigurationPrinter(), args)
    System.exit(exitCode)
  }
}
