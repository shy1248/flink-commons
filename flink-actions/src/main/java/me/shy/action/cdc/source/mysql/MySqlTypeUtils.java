package me.shy.action.cdc.source.mysql;

import static me.shy.action.cdc.source.mysql.MySqlActionUtils.MYSQL_CONVERTER_TINYINT1_BOOL;

import com.esri.core.geometry.ogc.OGCGeometry;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;

public class MySqlTypeUtils {
    // ------ MySQL Type ------
    // https://dev.mysql.com/doc/refman/8.0/en/data-types.html
    private static final String BIT = "BIT";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    private static final String REAL = "REAL";
    private static final String REAL_UNSIGNED = "REAL UNSIGNED";
    private static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    private static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL =
            "DOUBLE PRECISION UNSIGNED ZEROFILL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    private static final String FIXED = "FIXED";
    private static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    private static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String SET = "SET";
    private static final String ENUM = "ENUM";
    private static final String GEOMETRY = "GEOMETRY";
    private static final String POINT = "POINT";
    private static final String LINESTRING = "LINESTRING";
    private static final String POLYGON = "POLYGON";
    private static final String MULTIPOINT = "MULTIPOINT";
    private static final String MULTILINESTRING = "MULTILINESTRING";
    private static final String MULTIPOLYGON = "MULTIPOLYGON";
    private static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    private static final String UNKNOWN = "UNKNOWN";

    // This length is from JDBC.
    // It returns the number of characters when converting this timestamp to string.
    // The base length of a timestamp is 19, for example "2023-03-23 17:20:00".
    private static final int JDBC_TIMESTAMP_BASE_LENGTH = 19;

    private static final String LEFT_BRACKETS = "(";
    private static final String RIGHT_BRACKETS = ")";
    private static final String COMMA = ",";

    private static final List<String> HAVE_SCALE_LIST =
            Arrays.asList(DECIMAL, NUMERIC, DOUBLE, REAL, FIXED);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Type toIcebergType(String mysqlType) {
        return toIcebergType(
                MySqlTypeUtils.getShortType(mysqlType),
                MySqlTypeUtils.getPrecision(mysqlType),
                MySqlTypeUtils.getScale(mysqlType),
                MYSQL_CONVERTER_TINYINT1_BOOL.defaultValue());
    }

    public static Type toIcebergType(
            String type,
            @Nullable Integer length,
            @Nullable Integer scale,
            Boolean tinyInt1ToBool) {
        switch (type.toUpperCase()) {
            case BIT:
                if (length == null || length == 1) {
                    return Types.BooleanType.get();
                } else {
                    return Types.BinaryType.get();
                }
            case BOOLEAN:
            case BOOL:
                return Types.BooleanType.get();
            case TINYINT:
                // MySQL haven't boolean type, it uses tinyint(1) to represents boolean type
                // user should not use tinyint(1) to store number although jdbc url parameter
                // tinyInt1isBit=false can help change the return value, it's not a general way.
                // mybatis and mysql-connector-java map tinyint(1) to boolean by default, we behave
                // the same way by default. To store number (-128~127), we can set the parameter
                // tinyInt1ToByte (option 'mysql.converter.tinyint1-to-bool') to false, then
                // tinyint(1)
                // will be mapped to TinyInt.
                return length != null && length == 1 && tinyInt1ToBool
                        ? Types.BooleanType.get()
                        : Types.IntegerType.get();
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return Types.IntegerType.get();
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case MEDIUMINT:
            case YEAR:
                return Types.IntegerType.get();
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case BIGINT:
                return Types.LongType.get();
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return Types.DecimalType.of(20, 0);
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return Types.FloatType.get();
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return Types.DoubleType.get();
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return length != null && length <= 38
                        ? Types.DecimalType.of(length, scale != null && scale >= 0 ? scale : 0)
                        : Types.StringType.get();
            case DATE:
                return Types.DateType.get();
            case TIME:
                return Types.TimeType.get();
            case DATETIME:
            case TIMESTAMP:
                return Types.TimestampType.withZone();
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
            case GEOMETRYCOLLECTION:
                return Types.StringType.get();
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return Types.BinaryType.get();
            case SET:
                return Types.ListType.ofRequired(0, Types.StringType.get());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", type));
        }
    }

    public static boolean isGeoType(String type) {
        switch (type.toUpperCase()) {
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
            case GEOMETRYCOLLECTION:
                return true;
            default:
                return false;
        }
    }

    public static String convertWkbArray(byte[] wkb) throws JsonProcessingException {
        String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
        JsonNode originGeoNode = objectMapper.readTree(geoJson);

        Optional<Integer> srid =
                Optional.ofNullable(
                        originGeoNode.has("srid") ? originGeoNode.get("srid").intValue() : null);
        Map<String, Object> geometryInfo = new HashMap<>();
        String geometryType = originGeoNode.get("type").asText();
        geometryInfo.put("type", geometryType);
        if (geometryType.equalsIgnoreCase("GeometryCollection")) {
            geometryInfo.put("geometries", originGeoNode.get("geometries"));
        } else {
            geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
        }
        geometryInfo.put("srid", srid.orElse(0));
        ObjectWriter objectWriter = objectMapper.writer();
        return objectWriter.writeValueAsString(geometryInfo);
    }

    public static boolean isScaleType(String typeName) {
        return HAVE_SCALE_LIST.stream()
                .anyMatch(type -> getShortType(typeName).toUpperCase().startsWith(type));
    }

    public static boolean isEnumType(String typeName) {
        return typeName.toUpperCase().startsWith(ENUM);
    }

    public static boolean isSetType(String typeName) {
        return typeName.toUpperCase().startsWith(SET);
    }

    /* Get type after the brackets are removed.*/
    public static String getShortType(String typeName) {

        if (typeName.contains(LEFT_BRACKETS) && typeName.contains(RIGHT_BRACKETS)) {
            return typeName.substring(0, typeName.indexOf(LEFT_BRACKETS)).trim()
                    + typeName.substring(typeName.indexOf(RIGHT_BRACKETS) + 1);
        } else {
            return typeName;
        }
    }

    public static int getPrecision(String typeName) {
        if (typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && isScaleType(typeName)) {
            return Integer.parseInt(
                    typeName.substring(typeName.indexOf(LEFT_BRACKETS) + 1, typeName.indexOf(COMMA))
                            .trim());
        } else if ((typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && !isScaleType(typeName)
                && !isEnumType(typeName)
                && !isSetType(typeName))) {
            return Integer.parseInt(
                    typeName.substring(
                                    typeName.indexOf(LEFT_BRACKETS) + 1,
                                    typeName.indexOf(RIGHT_BRACKETS))
                            .trim());
        } else {
            return 0;
        }
    }

    public static int getScale(String typeName) {
        if (typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && isScaleType(typeName)) {
            return Integer.parseInt(
                    typeName.substring(
                                    typeName.indexOf(COMMA) + 1, typeName.indexOf(RIGHT_BRACKETS))
                            .trim());
        } else {
            return 0;
        }
    }
}
