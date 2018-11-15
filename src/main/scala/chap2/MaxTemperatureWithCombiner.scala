package chap2

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
  * Created by zhoudunxiong on 2018/11/15.
  */
object MaxTemperatureWithCombiner {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <out path>")
      System.exit(-1)
    }
    val job = new Job()
    job.setJarByClass(MaxTemperature.getClass)
    job.setJobName("Max temperature")

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setMapperClass(classOf[MaxTemperatureMapper])
    job.setCombinerClass(classOf[MaxTemperatureReducer])
    job.setReducerClass(classOf[MaxTemperatureReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    System.exit(
      if(job.waitForCompletion(true)) 0 else 1
    )
  }
}
