package com.example.flinkdemo.functions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {
    private MapState<String, Long> countState;
    MapStateDescriptor<String, Long> mapStateDesc = new MapStateDescriptor<String, Long>("mapStateDesc", String.class, Long.class);
    OutputTag<Tuple2<String, Long>> cumulativeOutputTag = new OutputTag<Tuple2<String, Long>>("cumulative-output1") {
    };

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        countState = getRuntimeContext().getMapState(mapStateDesc);
    }

    @Override
    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
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
