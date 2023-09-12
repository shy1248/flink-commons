package me.shy.action.cdc.test;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JsonRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomDebeziumDeserializationSchema.class);

    private final Boolean includeSchema;
    private transient JsonConverter jsonConverter;
    private Map<String, Object> customConverterConfigs;

    public CustomDebeziumDeserializationSchema() {
        this(false);
    }

    public CustomDebeziumDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    public CustomDebeziumDeserializationSchema(
            Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs = customConverterConfigs;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<JsonRecord> out) throws Exception {
        if (jsonConverter == null) {
            initializeJsonConverter();
        }

        LOG.info(
                "Record ==> " + new String(
                        jsonConverter.fromConnectData(
                                record.topic(), record.valueSchema(), record.value())));

        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        String tableName = record.topic();

        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            String insertMapString = extractAfterRow(value, valueSchema);
            JsonRecord jsonRecord = new JsonRecord(tableName, "INSERT", insertMapString);
            out.collect(jsonRecord);
        } else if (op == Envelope.Operation.DELETE) {
            String deleteString = extractBeforeRow(value, valueSchema);
            JsonRecord jsonRecord = new JsonRecord(tableName, "DELETE", deleteString);
            out.collect(jsonRecord);
        } else if (op == Envelope.Operation.UPDATE) {
            String updateString = extractAfterRow(value, valueSchema);
            JsonRecord jsonRecord = new JsonRecord(tableName, "UPDATE_AFTER", updateString);
            out.collect(jsonRecord);
        }
    }

    @Override
    public TypeInformation<JsonRecord> getProducedType() {
        return TypeInformation.of(new TypeHint<JsonRecord>() {
        });
    }


    private String extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Map<String, Object> map = getRowMap(after, afterSchema);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    private String extractBeforeRow(Struct value, Schema valueSchema)
            throws Exception {
        Struct beforeValue = value.getStruct(Envelope.FieldName.BEFORE);
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Map<String, Object> map = getRowMap(beforeValue, beforeSchema);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    private Map<String, Object> getRowMap(Struct value, Schema valueSchema) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : valueSchema.fields()) {
            map.put(field.name(), value.get(field.name()));
        }
        return map;
    }

    private void initializeJsonConverter() {
        jsonConverter = new JsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        if (customConverterConfigs != null) {
            configs.putAll(customConverterConfigs);
        }
        jsonConverter.configure(configs);
    }
}
