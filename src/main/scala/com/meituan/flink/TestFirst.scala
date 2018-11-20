package com.meituan.flink

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestFirst {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.readTextFile("D:\\finktest\\test.txt")
    import org.apache.flink.api.scala._
    val mapped = text.map{x => x.toInt}
    mapped.print()
    mapped.writeAsText("D:\\finktest\\test1.txt",WriteMode.OVERWRITE)
    env.execute("first example")
  }
}
