package chap4

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, WritableComparable}

/**
  * Created by zhoudunxiong on 2018/11/10.
  */
case class TextPair(first: Text = new Text(), second: Text = new Text()) extends WritableComparable[TextPair] {

  override def write(out: DataOutput): Unit = {
    first.write(out)
    second.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    first.readFields(in)
    second.readFields(in)
  }

  override def compareTo(o: TextPair): Int = {
    val cmp = first.compareTo(o.first)
    if (cmp != 0) cmp
    else second.compareTo(o.second)
  }

  override def toString: String = {
    first.toString + "\t" + second.toString
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case TextPair(f, s) => first.equals(f) && second.equals(s)
    }
  }

  override def hashCode(): Int = {
    first.hashCode() * 163 + second.hashCode()
  }
}
