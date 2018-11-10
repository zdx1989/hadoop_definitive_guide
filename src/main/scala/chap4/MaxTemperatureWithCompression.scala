package chap4

import org.apache.hadoop.mapreduce.Job

/**
  * Created by zhoudunxiong on 2018/11/10.
  */
object MaxTemperatureWithCompression {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> <output path>")
      System.exit(-1)
    }
    val job = new Job()
    //job.setJarByClass(Maxtemperature.class)
  }
}
