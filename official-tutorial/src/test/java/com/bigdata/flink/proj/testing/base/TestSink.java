package com.bigdata.flink.proj.testing.base;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class TestSink<OUT> implements SinkFunction<OUT> {

    // must be static
    public static final List VALUES = new ArrayList<>();

    @Override
    public void invoke(OUT value, Context context) {
        VALUES.add(value);
    }
}