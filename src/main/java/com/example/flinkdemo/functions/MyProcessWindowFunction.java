package com.example.flinkdemo.functions;

import com.example.flinkdemo.source.MyDataSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow> {

    private MyDataSource myDataSource;
    private MapState<String, Long> countState;

    public MyProcessWindowFunction(MyDataSource myDataSource) {
        this.myDataSource = myDataSource;
    }

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
                if (cumulativeCount >= 6) {
                    myDataSource.cancel();
                }
                cumulativeCount += count;
                countState.put(str, cumulativeCount);
                Tuple2<String, Long> result = new Tuple2<>(str, cumulativeCount);
                out.collect(result);
                if (cumulativeCount >= 6) {
                    ctx.output(cumulativeOutputTag, result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
