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
package com.inadco.hbl.scanner.functions;

import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.protocodegen.Cells.Aggregation.Builder;
import com.inadco.hbl.scanner.AggregateFunction;
import com.inadco.hbl.scanner.SliceOperation;

public class FCount implements AggregateFunction {
    
    
    
    @Override
    public void apply(Builder result, Double measure) {
        if ( measure == null)  return;
        long cnt=result.hasCnt()?result.getCnt():0;
        result.setCnt(cnt+1);
    }


    @Override
    public String getName() {
        return "COUNT";
    }
    

    @Override
    public boolean supportsComplementScan() {
        return true;
    }


    @Override
    public void merge(Builder accumulator, Aggregation source, SliceOperation operation) {
        if (!source.hasCnt())
            return;

        switch (operation) {
        case ADD:
            accumulator.setCnt(accumulator.hasCnt() ? accumulator.getCnt() + source.getCnt() : source.getCnt());
            break;
        case COMPLEMENT:
            accumulator.setCnt(accumulator.hasCnt() ? accumulator.getCnt() - source.getCnt() : -source.getCnt());
            break;
        }
    }


    @Override
    public double getDoubleValue(Aggregation source) {
        return source.hasCnt()?0:source.getCnt();
    }
    
    

}
