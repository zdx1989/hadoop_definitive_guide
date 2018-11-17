package chap5

import chap5.v1.MaxTemperatureReducer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver
import org.junit.Test

/**
  * Created by zhoudunxiong on 2018/11/17.
  */
class MaxTemperatureReducerTest {

  @Test
  def returnMaxIntegerInValue(): Unit = {
    import scala.collection.JavaConversions._

    val values = List(new IntWritable(12), new IntWritable(24), new IntWritable(15))
    new ReduceDriver[Text, IntWritable, Text, IntWritable]()
      .withReducer(new MaxTemperatureReducer())
      .withInput(new Text("1901"), values)
      .withOutput(new Text("1901"), new IntWritable(24))
      .runTest()
  }
}
