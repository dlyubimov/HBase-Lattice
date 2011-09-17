package com.inadco.hbl.client.impl.scanner;

import java.util.Arrays;

import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

public class RawScanResult implements Cloneable {
    
    private byte[] group;
    private Aggregation.Builder[] measures;
    
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
        RawScanResult result = (RawScanResult)super.clone();
        result.group=group.clone();
        // TODO
        // result.measures=
        throw new CloneNotSupportedException();
    }
    
    public void reset () {
        Arrays.fill(measures, null);
    }

    void mergeMeasures (RawScanResult other, AggregateFunctionRegistry afr ) { 
        for ( int i = 0; i < measures.length;i++)  {
            if ( other.measures[i]!= null ) { 
                if ( measures[i]==null)
                    measures[i]=other.measures[i];
                else { 
                    afr.mergeAll(measures[i], other.measures[i].clone().build(), SliceOperation.ADD);
                }
            }
        }
    }
    

}
