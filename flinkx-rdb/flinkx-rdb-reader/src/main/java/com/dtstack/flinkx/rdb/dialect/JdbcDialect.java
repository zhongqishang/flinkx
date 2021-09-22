package com.dtstack.flinkx.rdb.dialect;

import com.dtstack.flinkx.rdb.bean.TableColumn;
import com.dtstack.flinkx.rdb.utils.ObjectUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ROUND_CEILING;

public abstract class JdbcDialect implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    /**
     * The maximum evenly distribution factor used to judge the data in table is evenly distributed
     * or not, the factor could be calculated by MAX(id) - MIN(id) + 1 / rowCount.
     */
    public static final Double MAX_EVENLY_DISTRIBUTION_FACTOR = 2.0d;

    /**
     * Get the name of jdbc dialect.
     */
    public String dialectName() {
        return "";
    }

    /**
     * Quotes the identifier. This is used to put quotes around the identifier in case the column
     * name is a reserved keyword, or in case it contains characters that require quotes (e.g.
     * space). public using double quotes {@code "} to quote.
     */
    protected String quote(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Get limit clause to limit the number of emitted row from the jdbc source.
     *
     * @param limit number of row to emit. The value of the parameter should be non-negative.
     * @return the limit clause.
     */
    protected abstract String getLimitClause(long limit);

    public abstract DataType convertFromColumn(TableColumn column);

    public abstract Optional<TableColumn> getPkType(Connection connection, String tableName, String splitKey)
            throws SQLException;

    public boolean isSplitColumnEvenlyDistributed(String tableId,
                                                  TableColumn splitColumn,
                                                  Object min,
                                                  Object max,
                                                  int rowCnt) {
        // currently, we only support the optimization that split column with type BIGINT, INT,
        // DECIMAL
        DataType flinkType = convertFromColumn(splitColumn);
        LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();
        if (!(typeRoot == LogicalTypeRoot.BIGINT
                || typeRoot == LogicalTypeRoot.INTEGER
                || typeRoot == LogicalTypeRoot.DECIMAL)) {
            return false;
        }

        // only column is numeric and evenly distribution factor is less than
        // MAX_EVENLY_DISTRIBUTION_FACTOR will be treated as evenly distributed.
        final double evenlyDistributionFactor =
                calculateEvenlyDistributionFactor(min, max, rowCnt);
        LOG.info(
                "The evenly distribution factor for table {} is {}",
                tableId,
                evenlyDistributionFactor);
        return evenlyDistributionFactor <= MAX_EVENLY_DISTRIBUTION_FACTOR;
    }

    public int queryCount(Connection jdbc, String tableId)
            throws SQLException {
        final String countQuery =
                String.format("SELECT COUNT(1) FROM %s", quote(tableId));
        PreparedStatement statement = jdbc.prepareStatement(countQuery);
        ResultSet rs = statement.executeQuery();

        if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                    String.format("No result returned after running query [%s]", countQuery));
        }
        return rs.getInt(1);
    }

    public Object[] queryMinMax(Connection jdbc, String tableId, TableColumn splitColumn)
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

    public Object queryMin(
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

    public Object queryNextChunkMax(
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
        ResultSet rs = statement.executeQuery();
        if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                    String.format(
                            "No result returned after running query [%s]", query));
        }
        return rs.getObject(1);
    }

    public String buildSplitScanQuery(
            String tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, -1, true);
    }

    public String buildSplitQuery(
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

    public PreparedStatement readTableSplitDataStatement(
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

    public PreparedStatement initStatement(Connection connection, String sql, int fetchSize)
            throws SQLException {
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    public void addPrimaryKeyColumnsToCondition(
            RowType pkRowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
             fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate);
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    public String getPrimaryKeyColumnsProjection(RowType pkRowType) {
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

    public String getMaxPrimaryKeyColumnsProjection(RowType pkRowType) {
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

    public String buildSelectWithRowLimits(
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

    public String buildSelectWithBoundaryRowLimits(
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

    /**
     * Returns the evenly distribution factor of the table data.
     *
     * @param min               the min value of the split column
     * @param max               the max value of the split column
     * @param approximateRowCnt the approximate row count of the table.
     */
    private static double calculateEvenlyDistributionFactor(
            Object min, Object max, long approximateRowCnt) {
        if (!min.getClass().equals(max.getClass())) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported operation type, the MIN value type %s is different with MAX value type %s.",
                            min.getClass().getSimpleName(), max.getClass().getSimpleName()));
        }
        if (approximateRowCnt == 0) {
            return Double.MAX_VALUE;
        }
        BigDecimal difference = ObjectUtils.minus(max, min);
        // factor = max - min + 1 / rowCount
        final BigDecimal subRowCnt = difference.add(BigDecimal.valueOf(1));
        return subRowCnt.divide(new BigDecimal(approximateRowCnt), 2, ROUND_CEILING).doubleValue();
    }

}
