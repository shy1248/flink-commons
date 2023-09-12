package me.shy.action.cdc.source.mysql;

import static org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import io.debezium.data.Bits;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import me.shy.action.cdc.sink.iceberg.CdcRecord;
import me.shy.action.cdc.EventParser;
import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlDebeziumJsonEventParser  implements EventParser<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumJsonEventParser.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final TableNameConverter tableNameConverter;
    // private final List<ComputedColumn> computedColumns;
    // private final NewTableSchemaBuilder<JsonNode> schemaBuilder;
    @Nullable
    private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();

    private JsonNode root;
    private JsonNode payload;
    // NOTE: current table name is not converted by tableNameConverter
    private String currentTable;
    private boolean shouldSynchronizeCurrentTable;

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern) {
        this.serverTimeZone = serverTimeZone;
        this.caseSensitive = caseSensitive;
        this.tableNameConverter = tableNameConverter;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
    }

    @Override
    public void setRawEvent(String rawEvent) {
        try {
            root = objectMapper.readValue(rawEvent, JsonNode.class);
            payload = root.get("payload");
            currentTable = payload.get("source").get("table").asText();
            shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String parseTableName() {
        return tableNameConverter.convert(TableIdentifier.of(getDatabaseName(), currentTable));
    }

    private boolean isSchemaChange() {
        return payload.get("op") == null;
    }

    @Override
    public List<CdcRecord> parseRecords() {
        if (!shouldSynchronizeCurrentTable || isSchemaChange()) {
            return Collections.emptyList();
        }
        List<CdcRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(payload.get("before"));
        if (before.size() > 0) {
            before = caseSensitive ? before : keyCaseInsensitive(before);
            records.add(new CdcRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(payload.get("after"));
        if (after.size() > 0) {
            after = caseSensitive ? after : keyCaseInsensitive(after);
            records.add(new CdcRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private String getDatabaseName() {
        return payload.get("source").get("db").asText();
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        JsonNode schema =
                Preconditions.checkNotNull(
                        root.get("schema"),
                        "MySqlDebeziumJsonEventParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, String> mySqlFieldTypes = new HashMap<>();
        Map<String, String> fieldClassNames = new HashMap<>();
        JsonNode arrayNode = schema.get("fields");
        for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode elementNode = arrayNode.get(i);
            String field = elementNode.get("field").asText();
            if ("before".equals(field) || "after".equals(field)) {
                JsonNode innerArrayNode = elementNode.get("fields");
                for (int j = 0; j < innerArrayNode.size(); j++) {
                    JsonNode innerElementNode = innerArrayNode.get(j);
                    String fieldName = innerElementNode.get("field").asText();
                    String fieldType = innerElementNode.get("type").asText();
                    mySqlFieldTypes.put(fieldName, fieldType);
                    if (innerElementNode.get("name") != null) {
                        String className = innerElementNode.get("name").asText();
                        fieldClassNames.put(fieldName, className);
                    }
                }
            }
        }
        
        Map<String, Object> jsonMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> field : mySqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue();
            Object objectValue = jsonMap.get(fieldName);
            if (objectValue == null) {
                continue;
            }

            String className = fieldClassNames.get(fieldName);
            String oldValue = objectValue.toString();
            String newValue = oldValue;
            
            if (("bytes".equals(mySqlType) && className == null)
                    || Bits.LOGICAL_NAME.equals(className)) {
                // MySQL binary, varbinary, blob
                newValue = new String(Base64.getDecoder().decode(oldValue));
            } else if ("bytes".equals(mySqlType) && Decimal.LOGICAL_NAME.equals(className)) {
                // MySQL numeric, fixed, decimal
                try {
                    new BigDecimal(oldValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid big decimal value "
                                    + objectValue
                                    + ". Make sure that in the `customConverterConfigs` "
                                    + "of the JsonDebeziumDeserializationSchema you created, set '"
                                    + JsonConverterConfig.DECIMAL_FORMAT_CONFIG
                                    + "' to 'numeric'",
                            e);
                }
            } else if (Date.SCHEMA_NAME.equals(className)) {
                // MySQL date
                newValue = String.valueOf(DateTimeUtils.toLocalDate(Integer.parseInt(oldValue)));
            } else if (Timestamp.SCHEMA_NAME.equals(className)) {
                newValue = DateTimeUtils.formatTimestamp(TimestampData.fromEpochMillis(Long.parseLong(objectValue.toString())),
                        TimeZone.getDefault(), 3);
            } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
                newValue = DateTimeUtils.formatTimestamp(TimestampData.fromEpochMillis(Long.parseLong(objectValue.toString())),
                        TimeZone.getDefault(), 6);
            } else if (ZonedTimestamp.SCHEMA_NAME.equals(className)) {
                newValue = DateTimeUtils.formatTimestamp(TimestampData.fromEpochMillis(Long.parseLong(objectValue.toString())),
                        TimeZone.getTimeZone(serverTimeZone), 6);
            } else if (MicroTime.SCHEMA_NAME.equals(className)) {
                long microseconds = Long.parseLong(objectValue.toString());
                long microsecondsPerSecond = 1_000_000;
                long nanosecondsPerMicros = 1_000;
                long seconds = microseconds / microsecondsPerSecond;
                long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;
                newValue = Instant.ofEpochSecond(seconds, nanoAdjustment)
                                .atZone(ZoneOffset.UTC)
                                .toLocalTime()
                                .toString();
            } else if (Point.LOGICAL_NAME.equals(className)
                    || Geometry.LOGICAL_NAME.equals(className)) {
                JsonNode jsonNode = recordRow.get(fieldName);
                try {
                    byte[] wkb = jsonNode.get("wkb").binaryValue();
                    newValue = MySqlTypeUtils.convertWkbArray(wkb);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Failed to convert %s to geometry JSON.", jsonNode), e);
                }
            }

            resultMap.put(fieldName, newValue);
        }

        return resultMap;
    }

    private Map<String, String> keyCaseInsensitive(Map<String, String> origin) {
        Map<String, String> keyCaseInsensitive = new HashMap<>();
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String fieldName = entry.getKey().toLowerCase();
            checkArgument(
                    !keyCaseInsensitive.containsKey(fieldName),
                    "Duplicate key appears when converting map keys to case-insensitive form. Original map is:\n%s",
                    origin);
            keyCaseInsensitive.put(fieldName, entry.getValue());
        }
        return keyCaseInsensitive;
    }

    private boolean shouldSynchronizeCurrentTable() {
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        if (includedTables.contains(currentTable)) {
            return true;
        }

        boolean shouldSynchronize = true;
        if (includingPattern != null) {
            shouldSynchronize = includingPattern.matcher(currentTable).matches();
        }
        if (excludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !excludingPattern.matcher(currentTable).matches();
        }
        if (!shouldSynchronize) {
            LOG.debug(
                    "Source table {} won't be synchronized because it was excluded. ",
                    currentTable);
            excludedTables.add(currentTable);
            return false;
        }

        includedTables.add(currentTable);
        return true;
    }
}

