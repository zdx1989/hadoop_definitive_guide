package chap13

import java.io.IOException

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}


/**
  * Created by zhoudunxiong on 2018/11/25.
  */
object HBaseTemperatureImporter extends Configured with Tool {

  class HBaseTemperatureMapper extends Mapper[LongWritable, Text, Unit, Unit] {

    type MContext = Mapper[LongWritable, Text, Unit, Unit] # Context

    private var conn: Connection = _

    private var table: Table = _

    private val parser = new NcdcRecordParser()


    override def map(key: LongWritable, value: Text, context: MContext): Unit = {
      parser.parse(value.toString)
      if (parser.isValidTemperature()) {
        val rowKey = RowKeyConverter.makeObservationRowKey("1", 1L)
        val p = new Put(rowKey)
        p.addColumn(Bytes.toBytes("data"), Bytes.toBytes(1), Bytes.toBytes(parser.getAirTemperature()))
        table.put(p)
      }
    }

    override def setup(context: MContext): Unit = {
      super.setup(context)
      try {
        val conf = HBaseConfiguration.create(context.getConfiguration)
        conn = ConnectionFactory.createConnection(conf)
        val tableName = TableName.valueOf("observation")
        table = conn.getTable(tableName)
      } catch {
        case e: IOException => throw new RuntimeException("Failed HTable construct", e)
      }
    }

    override def cleanup(context: MContext): Unit = {
      super.cleanup(context)
      table.close()
      conn.close()
    }
  }

  override def run(args: Array[String]): Int = {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureImport <input>")
      return -1
    }
    val job = Job.getInstance()
    job.setJarByClass(getClass)
    job.setJobName("import temperature data to HBase")
    FileInputFormat.addInputPath(job, new Path(args(0)))
    job.setMapperClass(classOf[HBaseTemperatureMapper])
    job.setNumReduceTasks(0)
    job.setOutputFormatClass(classOf[NullOutputFormat[_, _]])
    if (job.waitForCompletion(true)) 0 else 1
  }

  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(HBaseConfiguration.create(), HBaseTemperatureImporter, args)
    System.exit(exitCode)
  }
}
