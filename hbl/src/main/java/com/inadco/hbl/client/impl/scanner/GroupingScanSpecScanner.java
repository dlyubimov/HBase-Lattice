package com.inadco.hbl.client.impl.scanner;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.datastructs.InputIterator;
import com.inadco.datastructs.adapters.GroupingIterator;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.impl.SliceOperation;

/**
 * This is a grouping scanner running on top of filtering scanner to merge scan
 * results into groups as request by scan spec.
 * <P>
 * 
 * It is meant to be run on a separate thread for optimum performance so results
 * could be combined later in the front end.
 * <P>
 * 
 * @deprecated use {@link GroupingIterator} with {@link GroupingScanStrategy}
 * @author dmitriy
 * 
 */
public class GroupingScanSpecScanner implements InputIterator<RawScanResult> {

    private ScanSpec                               scanSpec;

    private InputIterator<? extends RawScanResult> delegate;

    private RawScanResult                          nextTuple;
    private RawScanResult                          current;

    private AggregateFunctionRegistry              afr;
    private int                                    currentIndex = -1;
    private boolean                                applySliceOperation;

    public GroupingScanSpecScanner(ScanSpec scanSpec,
                                   InputIterator<? extends RawScanResult> ungroupedDelegate,
                                   AggregateFunctionRegistry afr,
                                   boolean applySliceOperation) {
        super();
        this.scanSpec = scanSpec;
        this.delegate = ungroupedDelegate;
        this.afr = afr;
        this.applySliceOperation = applySliceOperation;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() throws IOException {
        if (nextTuple == null)
            return delegate.hasNext();
        return true;

    }

    @Override
    public void next() throws IOException {
        if (!hasNext())
            throw new IOException("iterator at the end");

        if (current == null) {
            current = new RawScanResult(scanSpec);
            current.setSliceOperation(scanSpec.getSliceOperation());
        } else
            current.reset();

        if (nextTuple == null) {
            delegate.next();
            nextTuple = delegate.current();
        }
        System.arraycopy(nextTuple.getGroup(), 0, current.getGroup(), 0, scanSpec.getGroupKeyLen());
        do {
            current.mergeMeasures(nextTuple, afr, applySliceOperation ? nextTuple.getSliceOperation()
                : SliceOperation.ADD);

            if (!delegate.hasNext()) {
                nextTuple = null;
                break;
            }
            delegate.next();
            nextTuple = delegate.current();
            if (0 != Bytes.BYTES_RAWCOMPARATOR.compare(current.getGroup(),
                                                       0,
                                                       scanSpec.getGroupKeyLen(),
                                                       nextTuple.getGroup(),
                                                       0,
                                                       scanSpec.getGroupKeyLen()))
                break;

        } while (true);

    }

    @Override
    public RawScanResult current() throws IOException {
        return current;
    }

    @Override
    public int getCurrentIndex() throws IOException {
        return currentIndex;
    }

}
