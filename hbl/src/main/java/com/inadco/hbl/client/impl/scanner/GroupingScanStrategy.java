package com.inadco.hbl.client.impl.scanner;

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.datastructs.GroupingStrategy;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.impl.SliceOperation;

public class GroupingScanStrategy implements GroupingStrategy<RawScanResult,RawScanResult> {

    private ScanSpec                  scanSpec;
    private AggregateFunctionRegistry afr;
    private boolean                   applySliceOperation;
    private int                       groupKeyLen;

    public GroupingScanStrategy(ScanSpec scanSpec, AggregateFunctionRegistry afr, boolean applySliceOperation) {
        super();
        this.scanSpec = scanSpec;
        this.afr = afr;
        this.applySliceOperation = applySliceOperation;
        groupKeyLen = scanSpec.getGroupKeyLen();
    }

    @Override
    public boolean isItemInGroup(RawScanResult group, RawScanResult item) {

        return 0 == Bytes.BYTES_RAWCOMPARATOR.compare(group.getGroup(), 0, groupKeyLen, item.getGroup(), 0, groupKeyLen);
    }

    
    @Override
    public void initGroup(RawScanResult group, RawScanResult item) {
        byte[] grBytes=item.getGroup();
        System.arraycopy(grBytes,0,group.getGroup(),0,grBytes.length);
        
    }

    @Override
    public void aggregate(RawScanResult groupTo, RawScanResult item) {
        groupTo.mergeMeasures(item, afr, applySliceOperation?item.getSliceOperation():SliceOperation.ADD);
    }

    @Override
    public RawScanResult newGroupHolder(RawScanResult old) {
        if ( old == null ) { 
            RawScanResult newGroup=new RawScanResult(scanSpec);
            return newGroup;
        } else {
            old.reset();
            return old;
        }
    }

}
