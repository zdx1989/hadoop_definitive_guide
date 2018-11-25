package chap13

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by zhoudunxiong on 2018/11/25.
  */
object RowKeyConverter {

  private val STATION_ID_LENGTH = 12

  def makeObservationRowKey(stationID: String, observationTime: Long): Array[Byte] = {
    val row: Array[Byte] = Array()
    Bytes.putBytes(row, 0, Bytes.toBytes(stationID), 0, STATION_ID_LENGTH)
    val reverseOrderEpoch = Long.MaxValue - observationTime
    Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderEpoch)
    row
  }
}
