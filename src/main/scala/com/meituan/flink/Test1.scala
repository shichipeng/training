package com.meituan.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object Test1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val text: DataStream[String] = env.socketTextStream("hadoop02",9999)
    val counts =text.flatMap{_.toLowerCase.split("\\W+")filter(_.nonEmpty)}
      .map{(_,1)}
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("window Stream WordCount")

  }

}
