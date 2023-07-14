package com.example.flinkdemo.apps;

import com.example.flinkdemo.pojo.ItemViewCount;
import com.example.flinkdemo.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

public class HotItems {
    public static void main(String[] args) throws Exception {
        String filePath = "C:\\Users\\pengzheng\\IdeaProjects\\flink-demo\\src\\main\\resources\\UserBehavior.csv";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

//        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("/foo/bar")).build();
        SingleOutputStreamOperator<ItemViewCount> itemIdCount = env.readTextFile(filePath)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                .filter(value -> "pv".equals(value.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1L), Time.seconds(5)))
                .aggregate(new itemCountFunction(),new windowItemCount());

        itemIdCount
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(5))
                .print();

        env.execute("hot items job");

    }


    public static class itemCountFunction implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class windowItemCount implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Iterator<Long> iterator = input.iterator();
            Long count = iterator.next();
            long end = window.getEnd();
            out.collect(new ItemViewCount(itemId, end, count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private Integer topNumber;

        public TopNHotItems(Integer topNumber) {
            this.topNumber = topNumber;
        }

        ListState<ItemViewCount> itemViewCountState;

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountState.get().iterator());

            itemViewCounts.sort((o1, o2) -> {
//                return o2.f2 > o1.f2 ? 1 : -1;
                return o2.getCount().intValue() - o1.getCount().intValue();
            });

            StringBuilder result = new StringBuilder();
            result.append("======================================");
            result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            for (int i = 0; i < Math.min(topNumber, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                result.append("No ").append(i + 1).append(":")
                        .append("itemId = ").append(itemViewCount.getItemId())
                        .append("hot = ").append(itemViewCount.getCount())
                        .append("\n");
            }

            result.append("================================\n\n");

            Thread.sleep(1000L);

            out.collect(result.toString());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemViewCountState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemViewCountState", TypeInformation.of(new TypeHint<ItemViewCount>() {
            })));
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            itemViewCountState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }
    }
}


