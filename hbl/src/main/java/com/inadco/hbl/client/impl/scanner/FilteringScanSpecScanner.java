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

public class FilteringScanSpecScanner implements InputIterator<RawScanResult> {

    // caching. TODO: make this configurable.
    public static final int CACHING      = 128;

    private ScanSpec         scanSpec;

    private RawScanResult    next;
    private RawScanResult    current;
    private RawScanResult    holder;

    private ResultScanner    scanner;

    private Deque<Closeable> closeables   = new ArrayDeque<Closeable>();

    private int              currentIndex = -1;

    public FilteringScanSpecScanner(ScanSpec scanSpec, HTablePool tablePool) throws IOException {
        super();
        this.scanSpec = scanSpec;
        Validate.notNull(scanSpec);
        Validate.notEmpty(scanSpec.getMeasureQualifiers(), "scan requested no measures");

        byte[] tableName = Bytes.toBytes(scanSpec.getCuboid().getCuboidTableName());
        CompositeKeyRowFilter krf = new CompositeKeyRowFilter(scanSpec.getRanges());
        byte[] startRow = krf.getCompositeBound(true);
        byte[] endRow = krf.getCompositeBound(true);
        if (HblUtil.incrementKey(endRow, 0, endRow.length))
            endRow = null;

        Scan scan = new Scan();
        scan.setCaching(CACHING);
        scan.setStartRow(startRow);
        if (endRow != null)
            scan.setStopRow(endRow);

        scan.setFilter(krf);

        HTableInterface table = tablePool.getTable(tableName);
        Validate.notNull(table);
        closeables.addFirst(new IOUtil.PoolableHtableCloseable(tablePool, table));

        scanner = table.getScanner(scan);
        closeables.addFirst(scanner);
        
        closeables.remove(table);
        tablePool.putTable(table);
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
                holder.getMeasures()[i] = null;
            else {
                Aggregation.Builder aggrB = Aggregation.newBuilder();
                aggrB.mergeFrom(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
            }
        }

        return holder;
    }
    
}
