import java.util.Properties

import flinktohbaseAndMysql.{HBaseSink, MysqlSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011}

object KafkaToSinkStreaming {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置启动检查点（很重要）
    env.enableCheckpointing(1000)
    // 设置为TimeCharacteristic.EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val p = new Properties()
    p.setProperty("bootstrap.servers", "node01:9092")
    p.setProperty("zookeeper.connect", "node01:2181")
    val input = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), p))

    // 自定义MysqlSink类，将数据Sink到mysql
    val sink = new MysqlSink("jdbc:mysql://192.168.65.20:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "123456")
    input.addSink(sink)

    // 自定义HBaseSink类，将数据Sink到HBase
    val hBaseSink = new HBaseSink("student", "info")
    input.addSink(hBaseSink)

    env.execute("KafkaToSinkStreaming")
  }
}
