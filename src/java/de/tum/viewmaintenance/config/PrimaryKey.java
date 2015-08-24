package de.tum.viewmaintenance.config;

/**
 * Created by sharza on 8/23/15.
 */
public class PrimaryKey {
    private String columnName;
    private String columnJavaType;
    private String columnInternalCassType;
    private String columnValueInString;


    @Override
    public String toString() {
        return "Column Name: " + columnName +
                "\n ColumnJavaType: " + columnJavaType +
                "\n ColumnInternalCassType: " + columnInternalCassType +
                "\n ColumnValueInString: " + columnValueInString;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnJavaType() {
        return columnJavaType;
    }

    public void setColumnJavaType(String columnJavaType) {
        this.columnJavaType = columnJavaType;
    }

    public String getColumnInternalCassType() {
        return columnInternalCassType;
    }

    public void setColumnInternalCassType(String columnInternalCassType) {
        this.columnInternalCassType = columnInternalCassType;
    }

    public String getColumnValueInString() {
        return columnValueInString;
    }

    public void setColumnValueInString(String columnValueInString) {
        this.columnValueInString = columnValueInString;
    }
}
