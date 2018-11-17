package chap5

import chap2.MaxTemperatureMapper
import org.junit._
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapDriver

/**
  * Created by zhoudunxiong on 2018/11/17.
  */
class MaxTemperatureMapperTest {

  @Test
  def processesValidRecord(): Unit = {
    val value = new Text("0029029070999991901010106004+64333+023450FM-12+0005" +
      "99999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999")
    new MapDriver[LongWritable, Text, Text, IntWritable]()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(1L), value)
      .withOutput(new Text("1901"), new IntWritable(-78))
      .runTest()
  }
}
