package chap5.v1

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

/**
  * Created by zhoudunxiong on 2018/11/17.
  */
class MaxTemperatureReducer extends Reducer[Text, IntWritable, Text, IntWritable]{

  type RContext = Reducer[Text, IntWritable, Text, IntWritable] # Context

  override def reduce(key: Text, values: Iterable[IntWritable], context: RContext): Unit = {
    import scala.collection.JavaConversions._

    val maxValue = values.foldLeft(Integer.MIN_VALUE) { (a, b) =>
      Math.max(a, b.get())
    }
    context.write(key, new IntWritable(maxValue))
  }
}
