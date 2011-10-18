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

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.datastructs.GroupingStrategy;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.impl.SliceOperation;

/**
 * Grouping strategy for scan groups. <P>
 * 
 * Given scan spec, merge adjacent tuples together that constitute a group.<P>
 * 
 * @author dmitriy
 *
 */
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
