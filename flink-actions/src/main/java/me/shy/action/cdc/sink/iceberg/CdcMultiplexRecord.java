package me.shy.action.cdc.sink.iceberg;

import java.io.Serializable;
import java.util.Objects;

public class CdcMultiplexRecord  implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final CdcRecord record;

    public CdcMultiplexRecord(String databaseName, String tableName, CdcRecord record) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.record = record;
    }

    public static CdcMultiplexRecord fromCdcRecord(
            String databaseName, String tableName, CdcRecord record) {
        return new CdcMultiplexRecord(databaseName, tableName, record);
    }

    public String databaseName() {
        return databaseName;
    }

    public String tableName() {
        return tableName;
    }

    public CdcRecord record() {
        return record;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CdcMultiplexRecord)) {
            return false;
        }

        CdcMultiplexRecord that = (CdcMultiplexRecord) o;
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(record, that.record);
    }

    @Override
    public String toString() {
        return databaseName + "." + tableName + " " + record.toString();
    }
}
