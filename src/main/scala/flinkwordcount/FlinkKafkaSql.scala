package flinkwordcount

import java.util.Properties

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream.{DataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object FlinkKafkaSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    properties.setProperty("zookeeper.connect", "node1:2181,node2:2181,node3:2181")
    properties.setProperty("group.id", "kafka06")
    properties.setProperty("auto.offset.reset", "earliest")
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]
    ("pt_data", new SimpleStringSchema, properties))
    println("开始执行~~~~~~")
    stream.map(x=>{
      val split = x.split(",")
      (split(0)+","+split(1))
    }).returns(Types.STRING).print()

    env.execute()


  }

}
