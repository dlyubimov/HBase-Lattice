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
package com.inadco.hbl.client.impl.functions;

import com.inadco.hbl.api.AggregateFunction;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.protocodegen.Cells.Aggregation.Builder;

public class FCount implements AggregateFunction {

    @Override
    public void apply(Builder result, Object measure) {

        if ( result.hasCnt()) { 
            if ( measure != null ) 
                result.setCnt(result.getCnt()+1l);
        } else { 
            result.setCnt(measure==null?0l:1l);
        }

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
    public Object getAggrValue(Aggregation source) {
        return source.hasCnt() ? source.getCnt() : 0;
    }

}
