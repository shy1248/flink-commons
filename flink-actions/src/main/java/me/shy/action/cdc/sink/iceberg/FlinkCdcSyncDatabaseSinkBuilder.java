package me.shy.action.cdc.sink.iceberg;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import me.shy.action.cdc.EventParser;
import me.shy.action.cdc.source.Identifier;
import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.flink.CatalogLoader;

public class FlinkCdcSyncDatabaseSinkBuilder <T> {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    // private List<FileStoreTable> tables = new ArrayList<>();

    @Nullable
    private Integer parallelism;
    private CatalogLoader catalogLoader;
    private String database;
    private List<Identifier> tables = new ArrayList<>();

    public FlinkCdcSyncDatabaseSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withParserFactory(
            EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withTables(List<Identifier> tables) {
        this.tables = tables;
        return this;
    }


    public FlinkCdcSyncDatabaseSinkBuilder<T> withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withDatabase(String database) {
        this.database = database;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withCatalogLoader(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }
    

    public void build() {   
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);
        Preconditions.checkNotNull(database);
        Preconditions.checkNotNull(catalogLoader);
        buildSink();
    }

    private void buildSink() {
        Preconditions.checkNotNull(tables);
        // Create OutputSideTag every table
        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(new CdcMultiTableParsingProcessFunction<>(parserFactory))
                        .setParallelism(input.getParallelism());
        
        // Get OutputSide stream
        for (Identifier table: tables) {
            DataStream<CdcRecord> schemaProcessFunction =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                            parsed,
                            CdcMultiTableParsingProcessFunction.getRecordOutputTag(table.getFullName()));
            schemaProcessFunction.print(String.format("Table[%s]", table));
            // FlinkSink.builderFor(schemaChangeProcessFunction, new MapFunction<CdcRecord, RowData>() {
            //     @Override
            //     public RowData map(CdcRecord value) throws Exception {
            //         return null;
            //     }
            // }, TypeInformation.of(RowData.class)).upsert(true).append();
        }
    }
}
