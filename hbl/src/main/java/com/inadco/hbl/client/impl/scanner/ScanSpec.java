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

    public Range[] getRanges() {
        return ranges;
    }

    public Cuboid getCuboid() {
        return cuboid;
    }

    public int getNumGroupKeys() {
        return numGroupKeys;
    }

    public SliceOperation getSliceOperation() {
        return sliceOperation;
    }
    
    
}
