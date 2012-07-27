/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */
package com.inadco.hbl.client.impl.scanner;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.datastructs.InputIterator;
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.util.HblUtil;
import com.inadco.hbl.util.IOUtil;

/**
 * Filtered scan.
 * <P>
 * The idea is to have a custom filter that allows to put ranges on _each_ of
 * the composite keys. Then this scan figures out that filter's metadata and
 * runs it for the given scan spec.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class FilteringScanSpecScanner implements InputIterator<RawScanResult> {

    // caching. TODO: make this configurable.
    public static final int  CACHING      = 128;

    private ScanSpec         scanSpec;

    private RawScanResult    next;
    private RawScanResult    current;
    private RawScanResult    holder;

    private ResultScanner    scanner;

    private Deque<Closeable> closeables   = new ArrayDeque<Closeable>();

    private int              currentIndex = -1;

    /**
     * 
     * @param scanSpec
     * @param tablePool
     * @param splitStartKey
     *            optional: input split's requested beginning of the table
     * @param splitEndKey
     *            optional: input split's requested end of the table (half-open;
     *            null value means till the end of the table)
     * @param inputFormatTableName
     *            optional: input format's table name used to assert idempotency
     *            of execution accross all split tasks.
     * @throws IOException
     */
    public FilteringScanSpecScanner(ScanSpec scanSpec,
                                    HTablePool tablePool,
                                    byte[] splitStartKey,
                                    byte[] splitEndKey,
                                    String inputFormatTableName) throws IOException {
        super();
        this.scanSpec = scanSpec;
        Validate.notNull(scanSpec);
        Validate.notEmpty(scanSpec.getMeasureQualifiers(), "scan requested no measures");

        String tableName = scanSpec.getCuboid().getCuboidTableName();

        if (inputFormatTableName != null && !tableName.equals(inputFormatTableName))

            throw new IOException(
                String.format("Input format validation failed: expected table name %s from front end "
                    + "but different in the back end: %s.", inputFormatTableName, tableName));

        byte[] tableNameBytes = Bytes.toBytes(scanSpec.getCuboid().getCuboidTableName());
        CompositeKeyRowFilter krf = new CompositeKeyRowFilter(scanSpec.getRanges());
        byte[] startRow = krf.getCompositeBound(true);
        byte[] endRow = krf.getCompositeBound(false);
        if (HblUtil.incrementKey(endRow, 0, endRow.length))
            endRow = null;

        /*
         * process split constraints, if given.
         */
        if (splitStartKey != null) {
            if (Bytes.compareTo(startRow, splitStartKey) < 0)
                startRow = splitStartKey;
            if (splitEndKey != null) {
                if (endRow == null)
                    endRow = splitEndKey;
                else if (Bytes.compareTo(splitEndKey, endRow) > 0)
                    endRow = splitEndKey;
            }
            /*
             * as a result of such correction, it may happen (although should
             * not) that our correction for split resulted in a negative
             * interval.
             * 
             * if that's the case, then it means empty scan and we just fix it
             * by throwing end row to be the same as start.
             */
            if (endRow != null && Bytes.compareTo(endRow, startRow) < 0)
                endRow = startRow;
        }

        Scan scan = new Scan();
        scan.setCaching(CACHING);
        scan.setStartRow(startRow);
        if (endRow != null)
            scan.setStopRow(endRow);

        scan.setFilter(krf);

        HTableInterface table = tablePool.getTable(tableNameBytes);
        Validate.notNull(table);
        closeables.addFirst(new IOUtil.PoolableHtableCloseable(tablePool, table));

        scanner = table.getScanner(scan);
        closeables.addFirst(scanner);

        closeables.remove(table);
        /*
         * this has been deprecated in 0.92. use close() instead.
         * 
         * tablePool.putTable(table);
         */
        table.close();

    }

    @Override
    public void close() throws IOException {
        IOUtil.closeAll(closeables);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (next != null)
            return true;
        next = fetchNextRawResult(holder);
        if (next == null)
            return false;
        holder = null;
        return true;

    }

    @Override
    public void next() throws IOException {
        if (!hasNext())
            throw new IOException("At the end of the iterator");
        holder = current;
        current = next;
        next = null;
        currentIndex++;

    }

    @Override
    public RawScanResult current() throws IOException {
        return current;
    }

    @Override
    public int getCurrentIndex() throws IOException {
        return currentIndex;
    }

    public ScanSpec getScanSpec() {
        return scanSpec;
    }

    private RawScanResult fetchNextRawResult(RawScanResult holder) throws IOException {
        Result r = scanner.next();
        if (r == null)
            return null;

        if (holder == null)
            holder = new RawScanResult(scanSpec);

        byte[] row = r.getRow();
        Validate.isTrue(row.length >= scanSpec.getGroupKeyLen());
        System.arraycopy(row, 0, holder.getGroup(), 0, scanSpec.getGroupKeyLen());

        int i = 0;
        for (byte[] measureQualifier : scanSpec.getMeasureQualifiers()) {
            KeyValue kv = r.getColumnLatest(HblAdmin.HBL_METRIC_FAMILY, measureQualifier);
            if (kv == null)
                holder.getMeasures()[i++] = null;
            else {
                Aggregation.Builder aggrB = Aggregation.newBuilder();
                aggrB.mergeFrom(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
                holder.getMeasures()[i++] = aggrB;
            }
        }

        return holder;
    }

}
