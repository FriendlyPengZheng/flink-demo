package com.example.flinkdemo.apps;

import com.example.flinkdemo.source.HoloDataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import scala.Tuple2;


import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class HoloSqlApp {
    public static void main(String[] args) throws Exception {
        HoloDataSource holoDataSource = new HoloDataSource();
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
            SingleOutputStreamOperator<Tuple2<String, Long>> dataStreamSource = env.addSource(holoDataSource)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)));
            // 传递过来的是一整个字段，schema不可用
            Schema schema = Schema.newBuilder()
                    .column("receive_minute", DataTypes.STRING())
                    .column("event_time", DataTypes.BIGINT())
                    .build();

            tableEnv.createTemporaryView("temp_view", dataStreamSource, $("receive_minute"), $("event_time").rowtime());
            tableEnv.sqlQuery("select receive_minute,event_time from temp_view limit 100").execute().print();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            holoDataSource.close();
        }
    }
}
