package me.shy.action.cdc.sink.iceberg;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

public class SingleOutputStreamOperatorUtils {
    public static <T> DataStream<T> getSideOutput(
            SingleOutputStreamOperator<?> input, OutputTag<T> outputTag) {
        return input.getSideOutput(outputTag);
    }
}
