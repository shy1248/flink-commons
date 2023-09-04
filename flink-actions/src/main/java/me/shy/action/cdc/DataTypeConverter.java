package me.shy.action.cdc;

public interface DataTypeConverter {
    
    <T> T toPlatformDataType(String type, int precision, int scale, TypeMapping typeMapping);

}
