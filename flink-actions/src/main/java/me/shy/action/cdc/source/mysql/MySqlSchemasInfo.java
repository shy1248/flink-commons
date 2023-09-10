package me.shy.action.cdc.source.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import me.shy.action.cdc.source.Identifier;

public class MySqlSchemasInfo {

    private final Map<Identifier, MySqlSchema> pkTableSchemas;
    private final Map<Identifier, MySqlSchema> nonPkTableSchemas;

    public MySqlSchemasInfo() {
        this.pkTableSchemas = new HashMap<>();
        this.nonPkTableSchemas = new HashMap<>();
    }

    public void addSchema(Identifier identifier, MySqlSchema mysqlSchema) {
        if (mysqlSchema.primaryKeys().isEmpty()) {
            nonPkTableSchemas.put(identifier, mysqlSchema);
        } else {
            pkTableSchemas.put(identifier, mysqlSchema);
        }
    }

    public List<Identifier> pkTables() {
        return new ArrayList<>(pkTableSchemas.keySet());
    }

    public List<Identifier> nonPkTables() {
        return new ArrayList<>(nonPkTableSchemas.keySet());
    }

    // only merge pk tables now
    public MySqlTableInfo mergeAll() {
        boolean initialized = false;
        AllMergedMySqlTableInfo merged = new AllMergedMySqlTableInfo();
        for (Map.Entry<Identifier, MySqlSchema> entry : pkTableSchemas.entrySet()) {
            Identifier id = entry.getKey();
            MySqlSchema schema = entry.getValue();
            if (!initialized) {
                merged.init(id, schema);
                initialized = true;
            } else {
                merged.merge(id, schema);
            }
        }
        return merged;
    }
    public List<MySqlTableInfo> toMySqlTableInfos(boolean mergeShards) {
        if (mergeShards) {
            return mergeShards();
        } else {
            return pkTableSchemas.entrySet().stream()
                    .map(e -> new UnmergedMySqlTableInfo(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }
    }

    private List<MySqlTableInfo> mergeShards() {
        Map<String, ShardsMergedMySqlTableInfo> nameSchemaMap = new HashMap<>();
        for (Map.Entry<Identifier, MySqlSchema> entry : pkTableSchemas.entrySet()) {
            Identifier id = entry.getKey();
            String tableName = id.getTableName();

            MySqlSchema toBeMerged = entry.getValue();
            ShardsMergedMySqlTableInfo current = nameSchemaMap.get(tableName);
            if (current == null) {
                current = new ShardsMergedMySqlTableInfo();
                current.init(id, toBeMerged);
                nameSchemaMap.put(tableName, current);
            } else {
                nameSchemaMap.put(tableName, current.merge(id, toBeMerged));
            }
        }

        return new ArrayList<>(nameSchemaMap.values());
    }
    
}

