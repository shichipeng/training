package com.meituan.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode


  object WordCount {

    def main(args: Array[String]) {
      val env = ExecutionEnvironment.createLocalEnvironment(1)

      //从本地读取文件
      val text: DataSet[String] = env.readTextFile("D:\\finktest\\test.txt")

      //单词统计
      import org.apache.flink.api.scala._ //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错

      val counts = text.flatMap { _.toLowerCase.split("\\n") filter { _.nonEmpty } }
        .map { (_, 1) }
        .groupBy(0)
        .sum(1)

      //输出结果
      counts.print()

      //保存结果到txt文件
      counts.writeAsText("D:\\finktest\\test1.txt", WriteMode.OVERWRITE)
      env.execute("Scala WordCount Example")
    }
  }

