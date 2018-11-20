package com.meituan.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * flink开发第一个例子
  *
  *
  */
object SocketWindowWordCount {

  def main(args: Array[String]) : Unit = {

    // the host and the port to connect to
    var hostname: String = "hadoop02"
    var port: Int = 9999


    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream(hostname, port)


    // parse the data, group it, window it, and aggregate the counts

    import org.apache.flink.api.scala._ //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错

    val windowCounts = text
      .flatMap { w => w.split(",") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")


    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)

}
