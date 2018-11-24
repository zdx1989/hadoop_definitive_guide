package chap13

import java.io.IOException

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.util.StringUtils

/**
  * Created by zhoudunxiong on 2018/11/24.
  */
object ExampleClient {

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()

    //create table
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val hTableName = TableName.valueOf("test")
    val hTableDesc = new HTableDescriptor(hTableName)
    val hColumnDesc = new HColumnDescriptor("data")
    hTableDesc.addFamily(hColumnDesc)
    admin.createTable(hTableDesc)

    val tableName = hTableDesc.getNameAsString
    val tables: Array[HTableDescriptor] = admin.listTables()
    if (tables.length != 1 || tableName != tables(0).getNameAsString) {
      throw new IOException("Failed create table")
    }

    //run some operation --a put, a get, and a scan -- against the table
    val hTable = conn.getTable(TableName.valueOf(tableName))
    val row1 = Bytes.toBytes("row1")
    val p1 = new Put(row1)
    val family = Bytes.toBytes("data")
    p1.addColumn(family, Bytes.toBytes(1), Bytes.toBytes("value1"))
    hTable.put(p1)

    val g = new Get(row1)
    val result = hTable.get(g)
    println(s"Get: ${new String(result.value())}")

    val s = new Scan()
    val scanner = hTable.getScanner(s)
    import scala.collection.JavaConversions._
    try {
      scanner.foreach { res =>
        println(s"Scan: ${new String(res.value())}")
      }
    } finally {
      scanner.close()
    }

    // drop the table
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))

    conn.close()
  }
}
