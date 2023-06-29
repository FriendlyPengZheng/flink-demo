package com.example.flinkdemo.apps;

import com.example.flinkdemo.source.MyDataSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.ImmutableMapState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.temporal.ValueRange;
import java.util.HashMap;

public class TimeWindowApp {

    final static OutputTag<Tuple2<String, Long>> cumulativeOutputTag = new OutputTag<Tuple2<String, Long>>("cumulative-output1") {
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> StringStream = env.addSource(new MyDataSource());

        SingleOutputStreamOperator<Tuple2<String, Long>> process = StringStream.map(value -> new Tuple2<String, Long>(value, 1L)).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })).keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new MyProcessWindowFunction());
//                .process(new MyKeyedProcessFunction());
//                .process(new MyProcessFunction());


        SideOutputDataStream<Tuple2<String, Long>> sideOutput = process.getSideOutput(cumulativeOutputTag);
        sideOutput.print();

//        process.print();

        env.execute();

    }


}

class MyProcessFunction extends ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
    private MapState<String, Long> countState;
    MapStateDescriptor<String, Long> mapStateDesc = new MapStateDescriptor<String, Long>("mapStateDesc", String.class, Long.class);
    OutputTag<Tuple2<String, Long>> cumulativeOutputTag = new OutputTag<Tuple2<String, Long>>("cumulative-output1") {
    };

    @Override
    public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        countState = getRuntimeContext().getMapState(mapStateDesc);
        String str = value.f0;
        Long count = value.f1;
        Long cumulativeCount = countState.get(str);
        if (cumulativeCount == null) cumulativeCount = 0L;

        cumulativeCount += count;
        countState.put(str, cumulativeCount);
        Tuple2<String, Long> result = new Tuple2<>(str, cumulativeCount);
        out.collect(result);

        if (cumulativeCount >= 50) ctx.output(cumulativeOutputTag, result);
    }

}

class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {
    private MapState<String, Long> countState;
    MapStateDescriptor<String, Long> mapStateDesc = new MapStateDescriptor<String, Long>("mapStateDesc", String.class, Long.class);
    OutputTag<Tuple2<String, Long>> cumulativeOutputTag = new OutputTag<Tuple2<String, Long>>("cumulative-output1") {
    };

    @Override
    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        countState = getRuntimeContext().getMapState(mapStateDesc);
        String str = value.f0;
        Long count = value.f1;
        Long cumulativeCount = countState.get(str);
        if (cumulativeCount == null) cumulativeCount = 0L;

        cumulativeCount += count;
        countState.put(str, cumulativeCount);
        Tuple2<String, Long> result = new Tuple2<>(str, cumulativeCount);
        out.collect(result);

        if (cumulativeCount >= 2) ctx.output(cumulativeOutputTag, result);
    }
}

class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow> {
    private MapState<String, Long> countState;
    MapStateDescriptor<String, Long> mapStateDesc = new MapStateDescriptor<String, Long>("mapStateDesc", String.class, Long.class);
    OutputTag<Tuple2<String, Long>> cumulativeOutputTag = new OutputTag<Tuple2<String, Long>>("cumulative-output1") {
    };

    @Override
    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context ctx, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
        countState = getRuntimeContext().getMapState(mapStateDesc);
        elements.forEach(value -> {
            try {
                String str = value.f0;
                Long count = value.f1;
                Long cumulativeCount = countState.get(str);
                if (cumulativeCount == null) cumulativeCount = 0L;

                cumulativeCount += count;
                countState.put(str, cumulativeCount);
                Tuple2<String, Long> result = new Tuple2<>(str, cumulativeCount);
                out.collect(result);
                if (cumulativeCount >= 10) {
                    ctx.output(cumulativeOutputTag, result);
                    System.out.println(ctx.currentProcessingTime());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
