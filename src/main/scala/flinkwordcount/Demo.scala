package flinkwordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 1.实时统计总计网购时间超过2个小时的女性网民信息。
  * 2.周末两天的日志文件第一列为姓名，第二列为性别，第三列为本次停留时间，单位为分钟，分隔符为“,”。
  *
  *  1.读取文本数据，生成相应DataStream，解析数据生成UserRecord信息。
  *  2.筛选女性网民上网时间数据信息。
  *  3.按照姓名、性别进行keyby操作，并汇总在一个时间窗口内每个女性上网时间。
  *  4.筛选连续上网时间超过阈值的用户，并获取结果
  */
object Demo {
  def main(args: Array[String]): Unit = {
    //StreamExecutionEnvironment 执行流程序的上下文。环境提供了控制作业执行的方法（例如设置并行性或容错/检查点参数）以及与外部世界交互（数据访问）。
    // 构造执行环境，使用eventTime处理窗口数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置使用eventtime，默认是使用processtime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度为1,默认并行度是当前机器的cpu数量
    env.setParallelism(1)
    // 读取文本数据流
      val firstStream = env.readTextFile("D:/test/log1.txt")//取出第一个路径加载数据
      val unionStream =firstStream.union(env.readTextFile("D:/test/log2.txt")) //drop: drop(n: Int): List[A] 丢弃前n个元素，返回剩下的元素

    // 数据转换，构造整个数据处理的逻辑，计算并得出结果打印出来
    unionStream.map(line=>{
      val elems = line.split(",")
      assert(elems.length == 3)
      val name = elems(0)
      val sexy = elems(1)
      val time = elems(2).toInt
      UserRecord(name, sexy, time)
    }) //// 将读入的字符串转化为 UserRecord 对象
      .assignTimestampsAndWatermarks(new Record2TimestampExtractor)
      .filter(_.sexy == "female")//过滤famal
      .keyBy("name", "sexy")//使用name和sexy
      .window(TumblingEventTimeWindows.of(Time.minutes(2))) //滚动事件时间窗口(tumbling event-time windows)
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime > 120).print()

    // 调用execute触发执行
    env.execute("FemaleInfoCollectionPrint scala")
  }
  // UserRecord数据结构的定义
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)
  // 构造继承AssignerWithPunctuatedWatermarks的类，用于设置eventTime以及waterMark
  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[UserRecord] {
    // add tag in the data of datastream elements在datastream元素的数据中添加标记
    override def extractTimestamp(element: UserRecord, previousTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready 给水印触发窗口执行，并使用该值检查窗口元素是否准备就绪
    def checkAndGetNextWatermark(lastElement: UserRecord,extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1)
    }
  }

}

