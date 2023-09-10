package me.shy.action.cdc.test;

import java.io.Serializable;

public class FieldInfo implements Serializable   {
    private String name;
    private String type;
    private String comment;
    private Integer precision;
    private Integer scale;

    public FieldInfo(String name, String type, String comment, Integer precision, Integer scale) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }


    @Override
    public String toString() {
        return "FieldInfo{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", comment='" + comment + '\'' +
                ", precision=" + precision +
                ", scale=" + scale +
                '}';
    }
}
