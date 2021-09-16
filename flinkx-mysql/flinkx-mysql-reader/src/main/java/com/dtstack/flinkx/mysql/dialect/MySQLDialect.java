package com.dtstack.flinkx.mysql.dialect;

import com.dtstack.flinkx.rdb.bean.TableColumn;
import com.dtstack.flinkx.rdb.dialect.JdbcDialect;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

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
public class MySQLDialect extends JdbcDialect {

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public String quote(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String getLimitClause(long limit) {
        return " LIMIT " + limit;
    }

    @Override
    public Optional<TableColumn> getPkType(Connection connection, String tableName, String splitKey) throws SQLException {
        // TiDB 自增 ID
        if ("_tidb_rowid".equalsIgnoreCase(splitKey)) {
            return Optional.of(new TableColumn(
                    "_tidb_rowid",
                    "BIGINT",
                    "BIGINT",
                    12,
                    12,
                    "",
                    "", "0", true));
        }
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(connection.getCatalog(), null, tableName, null);
        List<TableColumn> columns = new ArrayList<>();
        while (rs.next()) {
            TableColumn column = new TableColumn(
                    rs.getString("COLUMN_NAME"), //字段名
                    rs.getString("TYPE_NAME"), //字段类型
                    rs.getString("DATA_TYPE"), //字段类型
                    rs.getInt("COLUMN_SIZE"), //长度
                    rs.getInt("DECIMAL_DIGITS"), //小数位长度
                    rs.getString("COLUMN_DEF"), //默认值
                    rs.getString("REMARKS"), //注释
                    rs.getString("ORDINAL_POSITION"),//字段位置
                    rs.getBoolean("IS_AUTOINCREMENT")//是否自增
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
            case TINYINT_UNSIGNED:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case INT:
            case MEDIUMINT:
                return DataTypes.INT();
            case INT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
            case BIGINT:
                return DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DECIMAL:
                return DataTypes.DECIMAL(column.length(), column.scale());
            case TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
            case TIMESTAMP:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP();
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
            case BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support %s type '%s' yet.", dialectName(), typeName));
        }
    }

    // ------ MySQL Type ------
    private static final String BIT = "BIT";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String BIGINT = "BIGINT";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
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
    private static final String UNKNOWN = "UNKNOWN";

}
