package com.dtstack.flinkx.rdb.dialect;

import com.dtstack.flinkx.rdb.bean.TableColumn;
import com.dtstack.flinkx.rdb.utils.ObjectUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface JdbcDialect {

    /**
     * Get the name of jdbc dialect.
     */
    String dialectName();

    /**
     * @return the default driver class name, if user not configure the driver class name, then will
     * use this one.
     */
    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

    /**
     * Quotes the identifier. This is used to put quotes around the identifier in case the column
     * name is a reserved keyword, or in case it contains characters that require quotes (e.g.
     * space). Default using double quotes {@code "} to quote.
     */
    default String quote(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Get limit clause to limit the number of emitted row from the jdbc source.
     *
     * @param limit number of row to emit. The value of the parameter should be non-negative.
     * @return the limit clause.
     */
    String getLimitClause(long limit);

    DataType convertFromColumn(TableColumn column);

    Optional<TableColumn> getPkType(Connection connection, String tableName, String splitKey) throws SQLException;

    default boolean splitColumnEvenlyDistributed(TableColumn splitColumn) {
        // only column is auto-incremental are recognized as evenly distributed.
        // TODO: we may use MAX,MIN,COUNT to calculate the distribution in the future.
        if (splitColumn.isAutoincrement()) {
            DataType flinkType = convertFromColumn(splitColumn);
            LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();
            // currently, we only support split column with type BIGINT, INT, DECIMAL
            return typeRoot == LogicalTypeRoot.BIGINT
                    || typeRoot == LogicalTypeRoot.INTEGER
                    || typeRoot == LogicalTypeRoot.DECIMAL;
        } else {
            return false;
        }
    }

    default int queryCount(Connection jdbc, String tableId)
            throws SQLException {
        final String countQuery =
                String.format(
                        "SELECT COUNT(1) FROM %s",
                        quote(tableId));
        PreparedStatement statement = jdbc.prepareStatement(countQuery);
        ResultSet rs = statement.executeQuery();

        if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                    String.format(
                            "No result returned after running query [%s]",
                            countQuery));
        }
        return rs.getInt(1);
    }

    default Object[] queryMinMax(Connection jdbc, String tableId, TableColumn splitColumn)
            throws SQLException {
        String columnName = splitColumn.getColumnName();
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quote(columnName), quote(columnName), quote(tableId));
        PreparedStatement statement = jdbc.prepareStatement(minMaxQuery);
        ResultSet rs = statement.executeQuery();

        if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                    String.format(
                            "No result returned after running query [%s]",
                            minMaxQuery));
        }
        Object o1 = rs.getObject(1);
        Object o2 = rs.getObject(2);
        return new Object[]{o1, o2};
    }

    default Object queryMin(
            Connection jdbc, String tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quote(columnName), quote(tableId), quote(columnName));

        PreparedStatement statement = jdbc.prepareStatement(minQuery);
        statement.setObject(1, excludedLowerBound);
        ResultSet rs = statement.executeQuery();
        if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                    String.format(
                            "No result returned after running query [%s]", minQuery));
        }
        return rs.getObject(1);
    }

    default Object queryNextChunkMax(
            Connection jdbc,
            String tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound,
            Object min)
            throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s %s ? ORDER BY %s ASC "
                                + getLimitClause(chunkSize)
                                + ") AS T",
                        quotedColumn,
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        ObjectUtils.compare(includedLowerBound, min) == 0 ? ">=" : ">",
                        quotedColumn
                );

        PreparedStatement statement = jdbc.prepareStatement(query);
        statement.setObject(1, includedLowerBound);
        System.out.println(statement.toString());
        ResultSet rs = statement.executeQuery();
        if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                    String.format(
                            "No result returned after running query [%s]", query));
        }
        return rs.getObject(1);
    }

    default String buildSplitScanQuery(
            String tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, -1, true);
    }

    default String buildSplitQuery(
            String tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            int limitSize,
            boolean isScanningData) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
                sql.append(")");
            }
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
            condition = sql.toString();
        }

        if (isScanningData) {
            return buildSelectWithRowLimits(
                    tableId, limitSize, "*", Optional.ofNullable(condition), Optional.empty());
        } else {
            final String orderBy =
                    pkRowType.getFieldNames().stream().collect(Collectors.joining(", "));
            return buildSelectWithBoundaryRowLimits(
                    tableId,
                    limitSize,
                    getPrimaryKeyColumnsProjection(pkRowType),
                    getMaxPrimaryKeyColumnsProjection(pkRowType),
                    Optional.ofNullable(condition),
                    orderBy);
        }
    }

    default PreparedStatement readTableSplitDataStatement(
            Connection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    default PreparedStatement initStatement(Connection connection, String sql, int fetchSize)
            throws SQLException {
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    default void addPrimaryKeyColumnsToCondition(
            RowType pkRowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
             fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate);
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    default String getPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
             fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next());
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    default String getMaxPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
             fieldNamesIt.hasNext(); ) {
            sql.append("MAX(" + fieldNamesIt.next() + ")");
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    default String buildSelectWithRowLimits(
            String tableId,
            int limit,
            String projection,
            Optional<String> condition,
            Optional<String> orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quote(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        if (orderBy.isPresent()) {
            sql.append(" ORDER BY ").append(orderBy.get());
        }
        if (limit > 0) {
            sql.append(getLimitClause(limit));
        }
        return sql.toString();
    }

    default String buildSelectWithBoundaryRowLimits(
            String tableId,
            int limit,
            String projection,
            String maxColumnProjection,
            Optional<String> condition,
            String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(maxColumnProjection);
        sql.append(" FROM (");
        sql.append("SELECT ");
        sql.append(projection);
        sql.append(" FROM ");
        sql.append(quote(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy).append(getLimitClause(limit));
        sql.append(") T");
        return sql.toString();
    }

}
