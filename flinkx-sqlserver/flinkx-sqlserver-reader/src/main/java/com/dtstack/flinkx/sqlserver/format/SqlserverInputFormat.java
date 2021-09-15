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
package com.dtstack.flinkx.sqlserver.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.rdb.inputformat.JdbcEnhanceInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.sqlserver.dialect.SqlserverDialect;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlserverInputFormat extends JdbcEnhanceInputFormat {

    public SqlserverInputFormat() {
        dialect = new SqlserverDialect();
    }

    @Override
    public void validate() {
        try {
            DatabaseMetaData metaData = dbConn.getMetaData();
            // TODO CHECK PK & Unique
            List<String> pks = new ArrayList<>();
            // UNIQUE KEY
            ResultSet uniqueKeyRs;
            if (table.contains(".")) {
                String[] arr = table.split("\\.");
                uniqueKeyRs = metaData.getIndexInfo(null, arr[0], arr[1], true, false);
            } else {
                uniqueKeyRs = metaData.getIndexInfo(null, null, table, true, false);
            }
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
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);

        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if (obj != null) {
                    if (CollectionUtils.isNotEmpty(columnTypeList)) {
                        if ("bit".equalsIgnoreCase(columnTypeList.get(pos))) {
                            if (obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                    obj = clobToString(obj);
                }

                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    /**
     * 构建边界位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param location         边界位置(起始/结束)
     * @param operator         判断符( >, >=,  <)
     * @return
     */
    @Override
    protected String getLocationSql(String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;
        boolean isTimeType = ColumnType.isTimeType(incrementColType)
                || ColumnType.NVARCHAR.name().equals(incrementColType);
        if (isTimeType) {
            endTimeStr = getTimeStr(Long.parseLong(location), incrementColType);
            endLocationSql = incrementCol + operator + endTimeStr;
        } else if (ColumnType.isNumberType(incrementColType)) {
            endLocationSql = incrementCol + operator + location;
        } else {
            endTimeStr = String.format("'%s'", location);
            endLocationSql = incrementCol + operator + endTimeStr;
        }

        return endLocationSql;
    }

    /**
     * 构建时间边界字符串
     *
     * @param location         边界位置(起始/结束)
     * @param incrementColType 增量字段类型
     * @return
     */
    @Override
    protected String getTimeStr(Long location, String incrementColType) {
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 23);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }
}
