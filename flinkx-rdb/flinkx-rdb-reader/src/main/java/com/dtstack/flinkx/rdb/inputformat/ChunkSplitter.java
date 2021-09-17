package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.rdb.bean.TableColumn;
import com.dtstack.flinkx.rdb.dialect.JdbcDialect;
import com.dtstack.flinkx.rdb.utils.ObjectUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * @author zhongqs
 * @date 2021-09-10 10:33
 */
public class ChunkSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(ChunkSplitter.class);

    private final Connection jdbc;
    private final JdbcDialect dialect;
    private final String splitKey;
    private final TableColumn splitColumn;

    public ChunkSplitter(Connection jdbc, JdbcDialect dialect, String splitKey, TableColumn splitColumn) {
        this.jdbc = jdbc;
        this.dialect = dialect;
        this.splitKey = splitKey;
        this.splitColumn = splitColumn;
    }

    /**
     * Generates all snapshot splits (chunks) for the give table path.
     */
    public List<JdbcInputSplit> generateSplits(String tableId, int minNumSplits) {
        long start = System.currentTimeMillis();

        // use first field in primary key as the split key
        final List<ChunkRange> chunks;
        try {
            chunks = splitTableIntoChunks(tableId, minNumSplits);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to split chunks for table " + tableId, e);
        }

        // convert chunks into splits
        List<JdbcInputSplit> splits = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkRange chunk = chunks.get(i);
            JdbcInputSplit split = createSnapshotSplit(i, chunks.size(), chunk);
            splits.add(split);
        }

        long end = System.currentTimeMillis();
        LOG.info(
                "Split table {} into {} chunks, time cost: {}ms.",
                tableId,
                splits.size(),
                Duration.ofMillis(end - start));
        return splits;
    }

    private JdbcInputSplit createSnapshotSplit(int partition, int total, ChunkRange chunk) {
        // currently, we only support single split column
//        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
//        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
//        Map<TableId, TableChange> schema = new HashMap<>();
//        schema.put(tableId, mySqlSchema.getTableSchema(tableId));
        return new JdbcEnhanceInputSplit(
                partition,
                total,
                splitColumn,
                chunk);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private List<ChunkRange> splitTableIntoChunks(String tableId, int minNumSplits)
            throws SQLException {

        final Object[] minMaxOfSplitColumn = dialect.queryMinMax(jdbc, tableId, splitColumn);
        final Object min = minMaxOfSplitColumn[0];
        final Object max = minMaxOfSplitColumn[1];
        final int count = dialect.queryCount(jdbc, tableId);
        int chunkSize = (count - 1) / minNumSplits + 1;
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> chunks;
        if (dialect.isSplitColumnEvenlyDistributed(jdbc, tableId, splitColumn, min, max, chunkSize)) {
            // recalculate chunkSize
            chunkSize = (ObjectUtils.minus(max, min).add(new BigDecimal(1)))
                    .divide(new BigDecimal(minNumSplits), 0, BigDecimal.ROUND_UP).intValue();
            // use evenly-sized chunks which is much efficient
            chunks = splitEvenlySizedChunks(min, max, chunkSize);
        } else {
            // use unevenly-sized chunks which will request many queries and is not efficient.
            chunks = splitUnevenlySizedChunks(tableId, splitColumn, min, max, chunkSize);
        }


        return chunks;
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in chunkSize step size.
     */
    private List<ChunkRange> splitEvenlySizedChunks(Object min, Object max, int chunkSize) {

        if (ObjectUtils.compare(ObjectUtils.plus(min, chunkSize), max) > 0) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, chunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            chunkEnd = ObjectUtils.plus(chunkEnd, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    /**
     * Split table into unevenly sized chunks by continuously calculating next chunk max value.
     */
    private List<ChunkRange> splitUnevenlySizedChunks(
            String tableId, TableColumn splitColumn, Object min, Object max, int chunkSize) throws SQLException {
        String splitColumnName = splitColumn.getColumnName();
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(min, tableId, splitColumnName, min, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on MySQL server
            maySleep(count++);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(chunkEnd, tableId, splitColumnName, min, max, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object nextChunkEnd(
            Object previousChunkEnd, String tableId, String splitColumnName, Object min, Object max, int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                dialect.queryNextChunkMax(jdbc, tableId, splitColumnName, chunkSize, previousChunkEnd, min);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = dialect.queryMin(jdbc, tableId, splitColumnName, chunkEnd);
        }
        if (ObjectUtils.compare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    // ------------------------------------------------------------------------------------------

    private static void maySleep(int count) {
        // every 100 queries to sleep 1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
        }
    }
}
