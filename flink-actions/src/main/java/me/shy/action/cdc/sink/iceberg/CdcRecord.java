package me.shy.action.cdc.sink.iceberg;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.types.RowKind;

public class CdcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private RowKind kind;

    private final Map<String, String> fields;

    public CdcRecord(RowKind kind, Map<String, String> fields) {
        this.kind = kind;
        this.fields = fields;
    }

    public CdcRecord setRowKind(RowKind kind) {
        this.kind = kind;
        return this;
    }

    public static CdcRecord emptyRecord() {
        return new CdcRecord(RowKind.INSERT, Collections.emptyMap());
    }

    public RowKind kind() {
        return kind;
    }

    public Map<String, String> fields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CdcRecord)) {
            return false;
        }
        CdcRecord that = (CdcRecord) o;
        return Objects.equals(kind, that.kind) && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, fields);
    }

    @Override
    public String toString() {
        return kind.shortString() + " " + fields;
    }
}
