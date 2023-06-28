package com.example.flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class TestDemo {

    @Test
    void contextLoads() {
    }

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();

        // call the methods that you have implemented
        assertEquals(3L, incrementer.map(5L));
    }

    @Test
    public void testIncrementFlatMap() throws Exception {
        // instantiate your function
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();

        Collector<Long> collector = mock(Collector.class);

        // call the methods that you have implemented
        incrementer.flatMap(2L, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(5L);
    }

    public static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(Long record) throws Exception {
            return record + 1;
        }
    }

    public static class IncrementFlatMapFunction extends RichFlatMapFunction<Long, Long> {
        @Override
        public void flatMap(Long value, Collector out) throws Exception {
            out.collect(value + 1);
        }
    }
}
