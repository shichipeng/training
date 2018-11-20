package com.meituan.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @program: flinkstudy
 * @description: flink测试第一个例子
 * @author: shichipeng
 * @create: 2018-10-29 23:04
 **/
public class Test1 {

    public static void main(String[] args) throws Exception {
        /**
         * 获取环境信息
         *
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("hadoop02", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();
        env.execute("window wordCount");
        System.out.println("my first use it                 ");


    }


    public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>>{

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for(String word:s.split(" ")){
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }


}
