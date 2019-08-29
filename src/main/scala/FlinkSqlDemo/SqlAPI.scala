package FlinkSqlDemo


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.table.api.scala._
object SqlAPI {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val dataStream=env.fromElements(("team","凯尔特人"),("player","杰伦布朗"))
    //第一种创建表的方式
    val tables: Table = dataStream.toTable(tableEnv,'name,'desc)
    val res =tableEnv.sqlQuery(s"select desc from $tables")
    tableEnv.toAppendStream[(String)](res).print()

    //第二种创建表的方式
    //val tableName: Table = tableEnv.fromDataStream(dataStream,'name,'desc)
   // val table = tableEnv.sqlQuery(s"select * from $tableName")

    //第三种创建表的方式
//    tableEnv.registerDataStream("Celtics",dataStream,'name,'desc)
//   val table: Table = tableEnv.sqlQuery("select * from Celtics")
   // val rs: DataStream[(String,String)] = tableEnv.toAppendStream[(String,String)](tables)
    //rs.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(10))).trigger(CountTrigger.of(10))
   // rs.print()
    env.execute("SqlAPI")
  }
}
