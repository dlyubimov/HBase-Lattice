package com.inadco.hbl.client.impl.scanner;

import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.impl.SliceOperation;

public class ScanSpec {
    private Range[] ranges;
    
    // need this to filter hierarchy keys for depth
    private Cuboid cuboid;
    
    // number of left-hand keys to be used for grouping
    private int numGroupKeys;
    
    // this is not used by grouping scanner, but 
    // it is subsequently used by group merging iterator 
    private SliceOperation sliceOperation;

    public ScanSpec(Range[] ranges, Cuboid cuboid, int numGroupKeys, SliceOperation sliceOperation) {
        super();
        this.ranges = ranges;
        this.cuboid = cuboid;
        this.numGroupKeys = numGroupKeys;
        this.sliceOperation = sliceOperation;
    }
}
