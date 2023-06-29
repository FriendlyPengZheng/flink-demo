package com.example.flinkdemo.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyFlinkSink implements SinkFunction<Tuple2<String ,Long>> {

}
