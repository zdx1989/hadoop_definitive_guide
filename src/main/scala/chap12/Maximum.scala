package chap12

import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.IntWritable

/**
  * Created by zhoudunxiong on 2018/11/23.
  */
class Maximum extends UDAF {

  object MaximumIntEvaluator extends UDAFEvaluator {

    private var result: IntWritable = _

    override def init(): Unit = {
      result = null
    }

    def iterate(value: IntWritable): Boolean = {
      if (value == null)
        return true
      if (result == null)
        result.set(value.get())
      else
        result.set(Math.max(result.get(), value.get()))
      return true
    }

    def terminatePartial(): IntWritable = {
      result
    }

    def merge(other: IntWritable): Boolean = {
      iterate(other)
    }

    def terminate(): IntWritable = {
      result
    }
  }
}
