package de.tum.viewmaintenance.view_table_structure;

import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;

/**
 * Created by shazra on 6/21/15.
 */

public class Column {
    private String name;
    private String type; // Contains CQL3 datatype: either from the view config or set explicitly through dataType
    private boolean isPrimaryKey = false;
    private String constraint;
    private String dataType; // Contains CQL3 datatype
    private String correspondingColumn;
    private Object value;
    private String javaDataType;

    public String getJavaDataType() {
        return javaDataType;
    }

    public void setJavaDataType(String javaDataType) {
        this.javaDataType = javaDataType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getCorrespondingColumn() {
        return correspondingColumn;
    }

    public void setCorrespondingColumn(String correspondingColumn) {
        this.correspondingColumn = correspondingColumn;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
        if (type== null || type.isEmpty()) {
            type = dataType;
        }

        if (javaDataType==null || javaDataType.isEmpty()) {
            javaDataType = ViewMaintenanceUtilities.getJavaDataTypeFromCQL3DataType(type);
        }
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

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
    }

    @Override
    public String toString() {
        return "Column - name : " + name +
                ", type : " + type +
                ", CQL3dataType : " + dataType +
                ", javaDataType : " + javaDataType +
                ", value : " + value +
                ", isPrimaryKey : " + isPrimaryKey;
    }
}

