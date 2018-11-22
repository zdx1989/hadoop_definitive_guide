package chap12

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text


/**
  * Created by zhoudunxiong on 2018/11/22.
  */
class Strip extends UDF {

  private val result: Text = new Text()


  def evaluate(str: Text): Text = {
    if (str == null) null
    else {
      result.set(StringUtils.strip(str.toString))
      result
    }
  }

  def evaluate(str: Text, stripChars: String): Text = {
    if (str == null) null
    else {
      result.set(StringUtils.strip(str.toString, stripChars))
      result
    }
  }
}
