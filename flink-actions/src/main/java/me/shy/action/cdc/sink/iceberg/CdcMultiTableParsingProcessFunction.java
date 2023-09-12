package me.shy.action.cdc.sink.iceberg;

import java.util.HashMap;
import java.util.Map;
import me.shy.action.cdc.EventParser;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CdcMultiTableParsingProcessFunction <T> extends ProcessFunction<T, Void> {

    private final EventParser.Factory<T> parserFactory;

    private transient EventParser<T> parser;
    private final Map<String, OutputTag<CdcRecord>> recordOutputTags = new HashMap<>();

    public CdcMultiTableParsingProcessFunction(EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
    }

    @Override
    public void open(Configuration parameters) {
        parser = parserFactory.create();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) {
        parser.setRawEvent(raw);
        String tableName = parser.parseTableName();
        parser.parseRecords()
                .forEach(record -> context.output(getRecordOutputTag(tableName), record));
    }
    
    public OutputTag<CdcRecord> getRecordOutputTag(String tableName) {
        return recordOutputTags.computeIfAbsent(
                tableName, CdcMultiTableParsingProcessFunction::createRecordOutputTag);
    }

    public static OutputTag<CdcRecord> createRecordOutputTag(String tableName) {
        return new OutputTag<>(tableName, TypeInformation.of(CdcRecord.class));
    }
}

