package me.shy.action.cdc.sink.iceberg;

import me.shy.action.cdc.DataTypeConverter;
import me.shy.action.cdc.TypeMapping;
import org.apache.iceberg.types.Types;

/**
 * @author shy
 * @date 2023/09/05 20:29
 **/
public class IcebergDataTypeConverter implements DataTypeConverter {

    @Override
    public Types toPlatformDataType(String type, int precision, int scale, TypeMapping typeMapping) {
        return null;
    }
}
