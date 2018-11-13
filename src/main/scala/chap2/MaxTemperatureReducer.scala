package chap2

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

/**
  * Created by zhoudunxiong on 2018/11/13.
  */
class MaxTemperatureReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  def reduce(key: Text, values: Iterable[IntWritable], context: Context): Unit = {
    val valuesIterator = values.iterator()
    var maxValue = Integer.MIN_VALUE
    while (valuesIterator.hasNext) {
      maxValue = Math.max(maxValue, valuesIterator.next().get())
    }
    context.write(key, new IntWritable(maxValue))
  }

}
