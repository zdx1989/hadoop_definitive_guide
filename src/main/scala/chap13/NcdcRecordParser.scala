package chap13

import org.apache.hadoop.io.Text

/**
  * Created by zhoudunxiong on 2018/11/25.
  */
class NcdcRecordParser {

  private val MISSING_TEMPERATURE = 9999

  private var year: String = _

  private var airTemperature: Int = _

  private var quality: String = _


  def parse(record: String): Unit = {
    year = record.substring(15, 19)
    val airTemperatureString =
      if (record.charAt(87) == '+') record.substring(88, 92)
      else record.substring(87, 92)
    airTemperature = airTemperatureString.toInt
    quality = record.substring(92, 93)
  }

  def parse(record: Text): Unit = {
    parse(record.toString)
  }

  def isValidTemperature(): Boolean = {
    airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]")
  }

  def getYear(): String = year

  def getAirTemperature(): Int = airTemperature

}
