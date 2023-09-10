package me.shy.action.cdc.source;

public class JdbcField {
    private Integer index;
    private String name;
    private String type;
    private String comment;
    private Integer precision;
    private Integer scale;


    public JdbcField() {
    }
    
    public JdbcField(Integer index, String name, String type, 
            String comment, Integer precision, Integer scale) {
        this.index = index;
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    @Override
    public String toString() {
        return "JdbcField{" +
                "index=" + index +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", comment='" + comment + '\'' +
                ", precision=" + precision +
                ", scale=" + scale +
                '}';
    }
}
