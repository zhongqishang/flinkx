/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.rdb.bean.TableColumn;
import com.dtstack.flinkx.rdb.dialect.JdbcDialect;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * InputFormat for reading data from a database and generate Rows.
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcEnhanceInputFormat extends JdbcInputFormat {

    public JdbcDialect dialect;

    public ChunkSplitter chunkSplitter;

    public void validate() {
        try {
            DatabaseMetaData metaData = dbConn.getMetaData();
            // TODO CHECK PK & Unique
            List<String> pks = new ArrayList<>();
            // UNIQUE KEY
            ResultSet uniqueKeyRs = metaData.getIndexInfo(null, null, table, true, false);
            while (uniqueKeyRs.next()) {
                String uniqueKey = uniqueKeyRs.getString("COLUMN_NAME");
                String seq = uniqueKeyRs.getString("ORDINAL_POSITION");
                if ("1".equals(seq)) {
                    pks.add(uniqueKey);
                }
            }
            if (!pks.contains(splitKey)) {
                throw new FlinkRuntimeException("Not found index.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new FlinkRuntimeException("validate failed.", e);
        }
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        Optional<TableColumn> column;
        try {
            dbConn = getConnection();
            validate();
            column = dialect.getPkType(dbConn, table, splitKey);
        } catch (SQLException e) {
            LOG.error("Failed to open MySQL connection", e);
            throw new FlinkRuntimeException("Failed to open MySQL connection", e);
        }
        if (!column.isPresent()) {
            throw new RuntimeException("Not Found Split column.");
        }
        chunkSplitter = new ChunkSplitter(dbConn, dialect, splitKey, column.get());
        List<JdbcInputSplit> list = chunkSplitter.generateSplits(table, minNumSplits);
        JdbcInputSplit[] splits = list.toArray(new JdbcInputSplit[list.size()]);
        return splits;
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        ClassUtil.forName(driverName, getClass().getClassLoader());
        initMetric(inputSplit);
        if (!canReadData(inputSplit)) {
            LOG.warn("Not read data when the start location are equal to end location");
            hasNext = false;
            return;
        }
        querySql = buildQuerySql(inputSplit);
        try {
            executeQuery(((JdbcEnhanceInputSplit) inputSplit));
            if (!resultSet.isClosed()) {
                columnCount = resultSet.getMetaData().getColumnCount();
            }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
        if (splitWithRowCol) {
            columnCount = columnCount - 1;
        }
        checkSize(columnCount, metaColumns);
        columnTypeList = DbUtil.analyzeColumnType(resultSet, metaColumns);
        LOG.info("JdbcInputFormat[{}] open: end", jobName);
    }

    /**
     * 执行查询
     *
     * @param jdbcEnhanceInputSplit
     * @throws SQLException
     */
    protected void executeQuery(JdbcEnhanceInputSplit jdbcEnhanceInputSplit) throws SQLException {
        String startLocation = jdbcEnhanceInputSplit.getStartLocation();
        dbConn = getConnection();
        // 部分驱动需要关闭事务自动提交，fetchSize参数才会起作用
        dbConn.setAutoCommit(false);
        if (incrementConfig.isPolling()) {
            if (StringUtils.isBlank(startLocation)) {
                //从数据库中获取起始位置
                queryStartLocation();
            } else {
                ps = dbConn.prepareStatement(querySql, resultSetType, resultSetConcurrency);
                ps.setFetchSize(fetchSize);
                ps.setQueryTimeout(queryTimeOut);
                queryForPolling(startLocation);
            }
        } else {
            ps =  // dbConn.createStatement(resultSetType, resultSetConcurrency);
                    dialect.readTableSplitDataStatement(
                            dbConn,
                            querySql,
                            jdbcEnhanceInputSplit.getChunkRange().getChunkStart() == null,
                            jdbcEnhanceInputSplit.getChunkRange().getChunkEnd() == null,
                            new Object[]{jdbcEnhanceInputSplit.getChunkRange().getChunkStart()},
                            new Object[]{jdbcEnhanceInputSplit.getChunkRange().getChunkEnd()},
                            1,
                            fetchSize);
            ps.setQueryTimeout(queryTimeOut);

            LOG.warn("Executing SQL: '{}'", ps.toString());

            resultSet = ps.executeQuery();
            hasNext = resultSet.next();
        }
    }

    /**
     * 构造查询sql
     *
     * @param inputSplit 数据切片
     * @return 构建的sql字符串
     */
    @Override
    protected String buildQuerySql(InputSplit inputSplit) {
        //QuerySqlBuilder中构建的queryTemplate
        String querySql = queryTemplate;

        if (inputSplit == null) {
            LOG.warn("inputSplit = null, Executing sql is: '{}'", querySql);
            return querySql;
        }

        JdbcEnhanceInputSplit jdbcInputSplit = (JdbcEnhanceInputSplit) inputSplit;

        DataType dataType = dialect.convertFromColumn(jdbcInputSplit.getColumn());

        final String selectSql =
                dialect.buildSplitScanQuery(
                        table,
                        (RowType) ROW(FIELD(jdbcInputSplit.getColumn().getColumnName(), dataType)).getLogicalType(),
                        jdbcInputSplit.getChunkRange().getChunkStart() == null,
                        jdbcInputSplit.getChunkRange().getChunkEnd() == null);
        LOG.info(
                "For split '{}' of table {} using select statement: '{}'",
                splitKey,
                table,
                selectSql);

        // TODO 是否开启断点续传
        if (restoreConfig.isRestore()) {
            if (formatState == null) {
                querySql = querySql.replace(DbUtil.RESTORE_FILTER_PLACEHOLDER, StringUtils.EMPTY);

                if (incrementConfig.isIncrement()) {
                    querySql = buildIncrementSql(jdbcInputSplit, querySql);
                }
            } else {
                boolean useMaxFunc = incrementConfig.isUseMaxFunc();
                String startLocation = getLocation(restoreColumn.getType(), formatState.getState());
                if (StringUtils.isNotBlank(startLocation)) {
                    LOG.info("update startLocation, before = {}, after = {}", jdbcInputSplit.getStartLocation(), startLocation);
                    jdbcInputSplit.setStartLocation(startLocation);
                    useMaxFunc = false;
                }
                String restoreFilter = buildIncrementFilter(restoreColumn.getType(),
                        restoreColumn.getName(),
                        jdbcInputSplit.getStartLocation(),
                        jdbcInputSplit.getEndLocation(),
                        customSql,
                        useMaxFunc);

                if (StringUtils.isNotEmpty(restoreFilter)) {
                    restoreFilter = " and " + restoreFilter;
                }

                querySql = querySql.replace(DbUtil.RESTORE_FILTER_PLACEHOLDER, restoreFilter);
            }

            querySql = querySql.replace(DbUtil.INCREMENT_FILTER_PLACEHOLDER, StringUtils.EMPTY);
        } else if (incrementConfig.isIncrement()) {
            querySql = buildIncrementSql(jdbcInputSplit, querySql);
        }

        return selectSql;
    }

}