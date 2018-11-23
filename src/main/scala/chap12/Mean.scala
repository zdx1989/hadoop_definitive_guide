package chap12

import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.DoubleWritable

/**
  * Created by zhoudunxiong on 2018/11/23.
  */
class Mean extends UDAF {

  class MeanDoubleEvaluator extends UDAFEvaluator {

    case class PartialResult(sum: Double, count: Long)

    private var partial: PartialResult = _

    override def init(): Unit = {
      partial = null
    }

    def iterate(value: DoubleWritable): Boolean = {
      if (value == null)
        return true
      if (partial == null) {
        val sum = value.get()
        val count = partial.count + 1
        partial = PartialResult(sum, count)
      }
      true
    }

    def terminatePartial(): PartialResult = {
      partial
    }

    def merge(other: PartialResult): Boolean = {
      if (other == null)
        return true
      if (partial == null) {
        val sum = partial.sum + other.sum
        val count = partial.count + other.count
        partial = PartialResult(sum, count)
      }
      true
    }

    def terminate(): DoubleWritable = {
      if (partial == null)
        null
      else
        new DoubleWritable(partial.sum / partial.count)
    }
  }
}
