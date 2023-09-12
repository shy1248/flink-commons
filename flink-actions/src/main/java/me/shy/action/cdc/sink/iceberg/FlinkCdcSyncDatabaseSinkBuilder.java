package me.shy.action.cdc.sink.iceberg;

import java.util.ArrayList;
import java.util.Map;
import javax.annotation.Nullable;
import me.shy.action.cdc.EventParser;
import me.shy.action.util.SingleOutputStreamOperatorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types.NestedField;

public class FlinkCdcSyncDatabaseSinkBuilder <T> {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    @Nullable
    private int parallelism;
    private CatalogLoader catalogLoader;
    private String database;
    private Map<TableIdentifier, Schema> tables;

    public FlinkCdcSyncDatabaseSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withParserFactory(
            EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withTables(Map<TableIdentifier, Schema> tables) {
        this.tables = tables;
        return this;
    }


    public FlinkCdcSyncDatabaseSinkBuilder<T> withParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }
    public FlinkCdcSyncDatabaseSinkBuilder<T> withCatalogLoader(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withDatabase(String database) {
        this.database = database;
        return this;
    }

    public void build() {   
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);
        Preconditions.checkNotNull(database);
        buildSink();
    }

    private void buildSink() {
        Preconditions.checkNotNull(tables);
        // Create OutputSideTag for every table
        CdcMultiTableParsingProcessFunction<T> parsingProcessFunction = 
                new CdcMultiTableParsingProcessFunction<>(parserFactory);
        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(parsingProcessFunction)
                        .setParallelism(input.getParallelism());
        
        // Get OutputSide stream
        for (Map.Entry<TableIdentifier, Schema> table: tables.entrySet()) {
            TableIdentifier tableIdentifier = table.getKey();
            Schema schema = table.getValue();
            DataStream<CdcRecord> schemaProcessFunction =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                            parsed,
                            parsingProcessFunction.getRecordOutputTag(tableIdentifier.name()));
            
            FlinkSink.builderFor(schemaProcessFunction,
                    (MapFunction<CdcRecord, RowData>) value -> {
                        Map<String, String> fields = value.fields();
                        GenericRowData rowData = new GenericRowData(schema.columns().size());
                        for (int i = 0; i < schema.columns().size(); i++) {
                            NestedField nestedField = schema.columns().get(i);
                            rowData.setField(i, fields.get(nestedField.name().toLowerCase()));
                        }
                        return rowData;
                    }, TypeInformation.of(RowData.class))
                    .tableLoader(TableLoader.fromCatalog(catalogLoader, tableIdentifier))
                    .equalityFieldColumns(new ArrayList<>(schema.identifierFieldNames()))
                    .upsert(true)
                    .writeParallelism(parallelism)
                    .append();
        }
    }
}
