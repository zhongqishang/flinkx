package com.dtstack.flinkx.postgresql.dialect;

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
 * @date 2021-09-13 15:24
 */
public class PostgresqlDialect implements JdbcDialect, Serializable {
    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public boolean splitColumnEvenlyDistributed(TableColumn splitColumn) {
        String splitType = splitColumn.getTypeName();
        return "int2".equalsIgnoreCase(splitType) ||
                "int4".equalsIgnoreCase(splitType) ||
                "int8".equalsIgnoreCase(splitType) ||
                "numeric".equalsIgnoreCase(splitType);
    }

    @Override
    public String getLimitClause(long limit) {
        return " LIMIT " + limit;
    }

    @Override
    public Optional<TableColumn> getPkType(Connection connection, String tableName, String splitKey) throws SQLException {
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
                    rs.getString("REMARKS"), // 注释
                    rs.getString("ORDINAL_POSITION"), //字段位置
                    rs.getBoolean("IS_AUTOINCREMENT") //是否自增
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
            case BOOL:
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case INT2:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
            case INT4:
                return DataTypes.INT();
            case INT8:
            case BIGINT:
            case INTEGER:
                return DataTypes.BIGINT();
            case REAL:
            case FLOAT4:
                return DataTypes.FLOAT();
            case DOUBLE:
            case FLOAT8:
                return DataTypes.DOUBLE();
            case DECIMAL:
            case NUMERIC:
                return DataTypes.DECIMAL(column.length(), column.scale());
            case TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case TIMESTAMP:
                return column.length() >= 0
                        ? DataTypes.TIMESTAMP(column.length())
                        : DataTypes.TIMESTAMP();
            case CHAR:
            case BPCHAR:
                return DataTypes.CHAR(column.length());
            case VARCHAR:
                return DataTypes.VARCHAR(column.length());
            case TEXT:
                return DataTypes.STRING();
            case BYTEA:
                return DataTypes.BINARY(column.length());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support %s type '%s' yet.", dialectName(), typeName));
        }
    }

    // ------ PostgreSQL Type ------
    private static final String BIGINT = "bigint";
    private static final String INT8 = "int8";
    private static final String BIGSERIAL = "bigserial";
    private static final String BIT = "bit ";
    private static final String BIT_VARYING = "bit_varying ";
    private static final String BOOL = "bool";
    private static final String BOOLEAN = "boolean";
    private static final String BOX = "box";
    private static final String BYTEA = "bytea";
    private static final String CHAR = "char";
    private static final String BPCHAR = "bpchar";
    private static final String VARCHAR = "varchar";
    private static final String CIDR = "cidr";
    private static final String CIRCLE = "circle";
    private static final String DATE = "date";
    private static final String DOUBLE = "double";
    private static final String FLOAT8 = "float8";
    private static final String INET = "inet";
    private static final String INT = "int";
    private static final String INT4 = "int4";
    private static final String INTEGER = "integer";
    private static final String INTERVAL = "interval";
    private static final String JSON = "json";
    private static final String JSONB = "jsonb";
    private static final String LINE = "line";
    private static final String LSEG = "lseg";
    private static final String MACADDR = "macaddr";
    private static final String MACADDR8 = "macaddr8";
    private static final String MONEY = "money";
    private static final String NUMERIC = "numeric";
    private static final String DECIMAL = "decimal";
    private static final String PATH = "path";
    private static final String PG_LSN = "pg_lsn";
    private static final String PG_SNAPSHOT = "pg_snapshot";
    private static final String POINT = "point";
    private static final String POLYGON = "polygon";
    private static final String REAL = "real";
    private static final String FLOAT4 = "float4";
    private static final String INT2 = "int2";
    private static final String SMALLINT = "smallint";
    private static final String SMALLSERIAL = "smallserial";
    private static final String SERIAL = "serial";
    private static final String TEXT = "text";
    private static final String TIME = "time";
    private static final String TIMESTAMP = "timestamp";
    private static final String TSQUERY = "tsquery";
    private static final String TSVECTOR = "tsvector";
    private static final String TXID_SNAPSHOT = "txid_snapshot";
    private static final String UUID = "uuid";
    private static final String XML = "xml";
}
