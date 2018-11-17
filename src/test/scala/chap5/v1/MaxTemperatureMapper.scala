package chap5.v1

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
  * Created by zhoudunxiong on 2018/11/17.
  */
class MaxTemperatureMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  type MConText = Mapper[LongWritable, Text, Text, IntWritable] # Context

  override def map(key: LongWritable, value: Text, context: MConText): Unit = {
    val line = value.toString
    val year = line.substring(15, 19)
    val airTemperature = line.substring(87, 92)
    if (!missing(airTemperature)) {
      context.write(new Text(year), new IntWritable(airTemperature.toInt))
    }
  }

  private def missing(temp: String): Boolean = {
    temp == "+9999"
  }
}
