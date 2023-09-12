package me.shy.action.cdc.source.mysql;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import me.shy.action.cdc.source.JdbcField;

public class MySqlSchema {
    private final LinkedHashMap<String, JdbcField> fields;
    private final List<String> primaryKeys;

    private MySqlSchema(
            LinkedHashMap<String, JdbcField> fields, List<String> primaryKeys) {
        this.fields = fields;
        this.primaryKeys = primaryKeys;
    }

    public static MySqlSchema buildSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String tableName)
            throws SQLException {
        LinkedHashMap<String, JdbcField> fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(
                databaseName, null, tableName, null)) {
            int index = 1;
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                String type = rs.getString("TYPE_NAME");
                String comment = rs.getString("REMARKS");

                Integer precision = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    precision = null;
                }
                
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                fields.put(name, new JdbcField(index, name, type, comment, precision, scale));
                index ++;
            }
        }

        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }

        return new MySqlSchema(fields, primaryKeys);
    }

    public LinkedHashMap<String, JdbcField> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public MySqlSchema merge(String currentTable, String otherTable, MySqlSchema other) {
        for (Map.Entry<String, JdbcField> entry : other.fields.entrySet()) {
            String fieldName = entry.getKey();
            String newType = entry.getValue().getType();
            if (fields.containsKey(fieldName)) {
                String oldType = fields.get(fieldName).getType();
                if (oldType.equalsIgnoreCase(newType)) {
                    fields.put(fieldName, other.fields.get(fieldName));
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Column %s have different types when merging schemas.\n"
                                            + "Current table '%s' fields: %s\n"
                                            + "To be merged table '%s' fields: %s",
                                    fieldName,
                                    currentTable,
                                    fieldsToString(),
                                    otherTable,
                                    other.fieldsToString()));
                }
            } else {
                fields.put(fieldName, other.fields.get(fieldName));
            }
        }

        if (!primaryKeys.equals(other.primaryKeys)) {
            primaryKeys.clear();
        }
        return this;
    }

    private String fieldsToString() {
        return "["
                + fields.entrySet().stream()
                .map(e -> String.format("%s", e))
                .collect(Collectors.joining(","))
                + "], Primary keys: [" + String.join(",", primaryKeys) + "]";
    }
}
