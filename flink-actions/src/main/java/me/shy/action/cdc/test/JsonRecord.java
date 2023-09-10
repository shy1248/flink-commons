package me.shy.action.cdc.test;

public class JsonRecord {

    private String tableName;
    private String op;
    private String fieldValues;

    public JsonRecord() {
    }

    public JsonRecord(String tableName, String op, String fieldValues) {
        this.tableName = tableName;
        this.op = op;
        this.fieldValues = fieldValues;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getFieldValues() {
        return fieldValues;
    }

    public void setFieldValues(String fieldValues) {
        this.fieldValues = fieldValues;
    }

    @Override
    public String toString() {
        return "JsonRecord{" +
                "tableName='" + tableName + '\'' +
                ", op='" + op + '\'' +
                ", fieldValues='" + fieldValues + '\'' +
                '}';
    }
}
