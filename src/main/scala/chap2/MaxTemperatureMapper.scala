package chap2

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
  * Created by zhoudunxiong on 2018/11/13.
  */
class MaxTemperatureMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  type WContext = Mapper[LongWritable, Text, Text, IntWritable] # Context

  private val MISSING: Int = 9999

  override def map(key: LongWritable, value: Text, context: WContext): Unit = {
    val line = value.toString
    val year = line.substring(15, 19)
    val airTemperature =
      if (line.charAt(87) == '+') line.substring(88, 92).toInt
      else line.substring(87, 92).toInt
    val quality = line.substring(92, 93)
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature))
    }
  }
}
