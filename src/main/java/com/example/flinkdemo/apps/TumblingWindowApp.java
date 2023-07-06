package com.example.flinkdemo.apps;

import com.example.flinkdemo.functions.MyProcessWindowFunction;
import com.example.flinkdemo.source.MyDataSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;


public class TumblingWindowApp {

    final static OutputTag<Tuple2<String, Long>> cumulativeOutputTag = new OutputTag<Tuple2<String, Long>>("cumulative-output1") {
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        MyDataSource myDataSource = MyDataSource.build();
        MyProcessWindowFunction myProcessWindowFunction = new MyProcessWindowFunction(myDataSource);
        DataStreamSource<String> StringStream = env.setParallelism(1).addSource(myDataSource).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> process = StringStream.map(value -> new Tuple2<String, Long>(value, 1L)).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })).keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(myProcessWindowFunction);

        SideOutputDataStream<Tuple2<String, Long>> sideOutput = process.getSideOutput(cumulativeOutputTag);
        sideOutput.print();

        process.print();
        env.execute();

    }
}
