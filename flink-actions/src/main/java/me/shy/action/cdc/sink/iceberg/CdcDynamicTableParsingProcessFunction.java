package me.shy.action.cdc.sink.iceberg;

import me.shy.action.cdc.EventParser;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcDynamicTableParsingProcessFunction<T> extends ProcessFunction<T, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CdcDynamicTableParsingProcessFunction.class);

    public static final OutputTag<CdcMultiplexRecord> DYNAMIC_OUTPUT_TAG =
            new OutputTag<>("iceberg-dynamic-table", TypeInformation.of(CdcMultiplexRecord.class));

    private final EventParser.Factory<T> parserFactory;
    private final String database;
    private transient EventParser<T> parser;

    public CdcDynamicTableParsingProcessFunction (
        String database, EventParser.Factory<T> parserFactory) {
    // for now, only support single database
        this.database = database;
        this.parserFactory = parserFactory;
}

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) throws Exception {
        parser.setRawEvent(raw);
        String tableName = parser.parseTableName();
        parser.parseRecords()
                .forEach(
                        record ->
                                context.output(
                                        DYNAMIC_OUTPUT_TAG,
                                        wrapRecord(database, tableName, record)));
    }

    private CdcMultiplexRecord wrapRecord(String databaseName, String tableName, CdcRecord record) {
        return CdcMultiplexRecord.fromCdcRecord(databaseName, tableName, record);
    }
}
