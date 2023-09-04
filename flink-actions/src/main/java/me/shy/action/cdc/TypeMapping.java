package me.shy.action.cdc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author shy
 * @date 2023/09/04 21:21
 **/
public class TypeMapping implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Set<TypeMappingMode> typeMappingModes;
    
    public TypeMapping(Set<TypeMappingMode> typeMappingModes) {
        this.typeMappingModes = typeMappingModes;
    }
    
    public boolean containsMode(TypeMappingMode mode) {
        return typeMappingModes.contains(mode);
    }
    
    public static Set<TypeMappingMode> defaultMapping() {
        return Collections.emptySet();
    }
    
    public static TypeMapping parse(String[] origOptions) {
        List<String> options = Arrays.stream(origOptions)
                .map(String::trim)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        Set<TypeMappingMode> typeMappingModes = new HashSet<>();
        for (String option: options) {
            switch (option) {
                case "tinyint1-not-bool":
                    typeMappingModes.add(TypeMappingMode.TINYINT1_NOT_BOOL);
                    break;
                case "to-nullable":
                    typeMappingModes.add(TypeMappingMode.TO_NULLABLE);
                    break;
                case "to-string":
                    typeMappingModes.add(TypeMappingMode.TO_STRING);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unknown type mapping option: %s.", option));
            }
        }
        return new TypeMapping(typeMappingModes);
    }
    

    public enum TypeMappingMode {
        TINYINT1_NOT_BOOL,
        TO_NULLABLE,
        TO_STRING;

        public String configString() {
            return name().toLowerCase().replace("_", "-");
        }
    }
}
