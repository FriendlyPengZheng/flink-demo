package com.example.flinkdemo.apps;

import com.example.flinkdemo.source.MyDataSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class TumblingWindowApp2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        MyDataSource myDataSource = MyDataSource.build();
        DataStreamSource<String> StringStream = env.setParallelism(4).addSource(myDataSource);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = StringStream.map(value -> new Tuple2<String, Long>(value, 1L)).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })).keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);


        sum.print();
        env.execute();

    }
}
