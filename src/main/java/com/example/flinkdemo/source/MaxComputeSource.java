package com.example.flinkdemo.source;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MaxComputeSource implements SourceFunction {

    @Override
    public void run(SourceContext ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }

    // todo read holo real time data and process

}
