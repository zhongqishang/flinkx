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

/**
 * @author jiangbo
 * @explanation
 * @date 2019/3/6
 */
public class JdbcEnhanceInputSplit extends JdbcInputSplit {
    private int mod;

    private String endLocation;

    private String startLocation;

    private TableColumn column;

    private ChunkRange chunkRange;

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public JdbcEnhanceInputSplit(int partitionNumber, int totalNumberOfPartitions, TableColumn column, ChunkRange chunkRange) {
        super(partitionNumber, totalNumberOfPartitions, 0, null, null);
        this.column = column;
        this.chunkRange = chunkRange;
    }

    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public JdbcEnhanceInputSplit(int partitionNumber, int totalNumberOfPartitions, int mod, String startLocation, String endLocation) {
        super(partitionNumber, totalNumberOfPartitions, mod, startLocation, endLocation);
    }

    public ChunkRange getChunkRange() {
        return chunkRange;
    }

    public void setChunkRange(ChunkRange chunkRange) {
        this.chunkRange = chunkRange;
    }

    public TableColumn getColumn() {
        return column;
    }

    public void setColumn(TableColumn column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "JdbcInputSplit{" +
                "mod=" + mod +
                ", endLocation='" + endLocation + "'" +
                ", startLocation='" + startLocation + "'" +
                ", chunkRange='" + chunkRange.getChunkStart() + " -> " + chunkRange.getChunkEnd() + "'" +
                '}';
    }
}
