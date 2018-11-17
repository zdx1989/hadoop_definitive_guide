package chap5.v1

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool, ToolRunner}

/**
  * Created by zhoudunxiong on 2018/11/17.
  */
class MaxTemperatureDriver extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>", getClass.getSimpleName)
      ToolRunner.printGenericCommandUsage(System.err)
      return -1
    }

    val job = new Job(getConf, "Max Configuration")
    job.setJarByClass(MaxTemperatureDriver.getClass)
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setMapperClass(classOf[MaxTemperatureMapper])
    job.setCombinerClass(classOf[MaxTemperatureReducer])
    job.setReducerClass(classOf[MaxTemperatureReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    if (job.waitForCompletion(true)) 0 else 1
  }
}

object MaxTemperatureDriver {

  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(new MaxTemperatureDriver(), args)
    System.exit(exitCode)
  }
}
