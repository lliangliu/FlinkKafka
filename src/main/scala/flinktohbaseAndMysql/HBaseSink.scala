package flinktohbaseAndMysql

import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[String] {


  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "node01,node02,node03")
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    conf.set("zookeeper.znode.parent","/hbase/master")
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val g = new Gson()
    val student = g.fromJson(value, classOf[Student])
    println(value)
    println(student)
    val t: Table = conn.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(Bytes.toBytes(student.sid))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(student.name))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(student.age))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes(student.sex))
    println(put)
    println(student.name+"  "+student.age+"  "+student.sex)
    t.put(put)
    println("提交成功")

  }

  override def close(): Unit = {

    super.close()
    conn.close()
  }
  case class Student(name: String, age: String, sex: String, sid: String)
}


