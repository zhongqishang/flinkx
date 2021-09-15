package com.dtstack.flinkx.sqlserver.dialect;

import com.dtstack.flinkx.rdb.bean.TableColumn;
import com.dtstack.flinkx.rdb.dialect.JdbcDialect;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author zhongqs
 * @date 2021-09-10 18:02
 */
public class SqlserverDialect implements JdbcDialect, Serializable {

    @Override
    public String dialectName() {
        return "SQL Server";
    }

    @Override
    public String getLimitClause(long limit) {
        return "OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY";
    }

    @Override
    public String quote(String identifier) {
        if (identifier.contains(".")) {
            return "\"" + identifier.replace(".", "\".\"") + "\"";
        }
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<TableColumn> getPkType(Connection connection, String tableName, String splitKey) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs;
        if (tableName.contains(".")) {
            String[] arr = tableName.split("\\.");
            rs = metaData.getColumns(connection.getCatalog(), arr[0], arr[1], null);
        } else {
            rs = metaData.getColumns(connection.getCatalog(), null, tableName, null);
        }
        List<TableColumn> columns = new ArrayList<>();
        while (rs.next()) {
            String typeName = rs.getString("TYPE_NAME");
            TableColumn column = new TableColumn(
                    rs.getString("COLUMN_NAME"), //字段名
                    typeName.replace(" identity", ""), //字段类型
                    rs.getString("DATA_TYPE"), //字段类型
                    rs.getInt("COLUMN_SIZE"), //长度
                    rs.getInt("DECIMAL_DIGITS"), //小数位长度
                    rs.getString("COLUMN_DEF"), //默认值
                    rs.getString("REMARKS"), //注释
                    rs.getString("ORDINAL_POSITION"),//字段位置
                    typeName.contains(" identity") //是否自增
            );
            columns.add(column);
        }
        Optional<TableColumn> pkType = columns.stream()
                .filter(col -> splitKey.equalsIgnoreCase(col.getColumnName()))
                .findFirst();
        return pkType;
    }

    @Override
    public DataType convertFromColumn(TableColumn column) {
        String typeName = column.getTypeName();
        switch (typeName) {
            case TINYINT:
                return column.length() == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case SMALLMONEY:
            case MONEY:
            case DECIMAL:
            case NUMERIC:
                return DataTypes.DECIMAL(column.length(), column.scale());
            case TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
            case DATETIME2:
                return DataTypes.TIMESTAMP();
            case CHAR:
                return DataTypes.CHAR(column.length());
            case VARCHAR:
                return DataTypes.VARCHAR(column.length());
            case TEXT:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(column.length());
            case VARBINARY:
                return DataTypes.VARBINARY(column.length());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support %s type '%s' yet.", dialectName(), typeName));
        }
    }

    // ------ SQL Server Type ------
    private static final String BIGINT = "bigint";
    private static final String BINARY = "binary";
    private static final String BIT = "bit";
    private static final String CHAR = "char";
    private static final String DATE = "date";
    private static final String DATETIME = "datetime";
    private static final String DATETIME2 = "datetime2";
    private static final String DATETIMEOFFSET = "datetimeoffset";
    private static final String DECIMAL = "decimal";
    private static final String FLOAT = "float";
    private static final String INT = "int";
    private static final String MONEY = "money";
    private static final String NCHAR = "nchar";
    private static final String NTEXT = "ntext";
    private static final String NUMERIC = "numeric";
    private static final String NVARCHAR = "nvarchar";
    private static final String REAL = "real";
    private static final String SMALLDATETIME = "smalldatetime";
    private static final String SMALLINT = "smallint";
    private static final String SMALLMONEY = "smallmoney";
    private static final String SQL_VARIANT = "sql_variant";
    private static final String TEXT = "text";
    private static final String TIME = "time";
    private static final String TINYINT = "tinyint";
    private static final String UNIQUEIDENTIFIER = "uniqueidentifier";
    private static final String VARBINARY = "varbinary";
    private static final String VARCHAR = "varchar";
}
