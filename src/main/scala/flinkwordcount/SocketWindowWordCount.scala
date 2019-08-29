package flinkwordcount

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val res: DataStream[String] = env.socketTextStream("node01", 9998, '\n')
    val wordCounts = res
      .flatMap { w => w.split(",") }
      .map((_, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    wordCounts.print()

    env.execute("SocketWindowWordCount")
  }
}
