package com.dtstack.flinkx.rdb.bean;

import java.io.Serializable;

/**
 * @author zhongqs
 * @date 2021-09-10 16:35
 */
public class TableColumn implements Serializable {
    private String columnName;
    private String typeName;
    private String dataType;
    private int columnSize;
    private int decimalDigits;
    private String columnDef;
    private String remarks;
    private String ordinalPosition;
    private boolean autoincrement;

    public TableColumn(String columnName, String typeName, String dataType, int columnSize, int decimalDigits, String columnDef, String remarks, String ordinalPosition, boolean autoincrement) {
        this.columnName = columnName;
        this.typeName = typeName;
        this.dataType = dataType;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.columnDef = columnDef;
        this.remarks = remarks;
        this.ordinalPosition = ordinalPosition;
        this.autoincrement = autoincrement;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public String getColumnDef() {
        return columnDef;
    }

    public void setColumnDef(String columnDef) {
        this.columnDef = columnDef;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(String ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public boolean isAutoincrement() {
        return autoincrement;
    }

    public void setAutoincrement(boolean autoincrement) {
        this.autoincrement = autoincrement;
    }

    public int length() {
        return columnSize;
    }

    public int scale() {
        return decimalDigits;
    }
}
