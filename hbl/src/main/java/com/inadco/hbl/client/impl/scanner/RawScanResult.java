package com.inadco.hbl.client.impl.scanner;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

public class RawScanResult implements Cloneable {

    private byte[]                group;
    private Aggregation.Builder[] measures;
    private SliceOperation        sliceOperation;

    public RawScanResult(ScanSpec ss) {
        super();
        setGroup(new byte[ss.getGroupKeyLen()]);
        setMeasures(new Aggregation.Builder[ss.getMeasureQualifiers().length]);

    }

    public byte[] getGroup() {
        return group;
    }

    public void setGroup(byte[] group) {
        this.group = group;
    }

    public Aggregation.Builder[] getMeasures() {
        return measures;
    }

    public void setMeasures(Aggregation.Builder[] measures) {
        this.measures = measures;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        RawScanResult result = (RawScanResult) super.clone();
        result.group = group.clone();
        // TODO
        // result.measures=
        throw new CloneNotSupportedException();
    }

    public void reset() {
        Arrays.fill(measures, null);
    }

    void mergeMeasures(RawScanResult other, AggregateFunctionRegistry afr, SliceOperation so ) {
        for (int i = 0; i < measures.length; i++) {
            if (other.measures[i] != null) {
                if (measures[i] == null)
                    measures[i] = other.measures[i];
                else {
                    afr.mergeAll(measures[i], other.measures[i].clone().build(), so);
                }
            }
        }
    }

    /**
     * to sort or sort-merge results by group
     * 
     * @author dmitriy
     * 
     */
    public static class GroupComparator implements Comparator<RawScanResult> {

        @Override
        public int compare(RawScanResult o1, RawScanResult o2) {
            return Bytes.BYTES_RAWCOMPARATOR.compare(o1.group, o2.group);
        }
    }

    public SliceOperation getSliceOperation() {
        return sliceOperation;
    }

    public void setSliceOperation(SliceOperation sliceOperation) {
        this.sliceOperation = sliceOperation;
    }
    
    

}
