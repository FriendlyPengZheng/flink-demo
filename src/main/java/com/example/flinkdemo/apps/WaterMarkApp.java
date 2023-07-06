package com.example.flinkdemo.apps;

import com.example.flinkdemo.source.MyDataSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WaterMarkApp {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        // 默认就是这个 不需要显式设置
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> inputStream = env.addSource(MyDataSource.build());

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = inputStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .map(value -> new Tuple2<String, Long>(value, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                }))
                .keyBy(value -> value.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1);

        sum.print();

        env.execute();
    }
}
