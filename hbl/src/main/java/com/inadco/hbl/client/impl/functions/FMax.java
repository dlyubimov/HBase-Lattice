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

import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.protocodegen.Cells.Aggregation.Builder;

/**
 * aggregate function of sum
 * 
 * @author dmitriy
 * 
 */
public class FMax extends AbstractAggregateFunc {

    public static final String FNAME = "MAX";

    public FMax() {
        super(FNAME);
    }

    @Override
    public boolean supportsComplementScan() {
        return false;
    }

    @Override
    public void merge(Builder accumulator, Aggregation source, SliceOperation operation) {
        if (!source.hasMax())
            return;

        switch (operation) {
        case ADD:
            if (!accumulator.hasMax() || accumulator.getMax() < source.getMax())
                accumulator.setMax(source.getMax());
            break;
        }
    }

    @Override
    public void apply(Builder result, Object measure) {
        if (!(measure instanceof Number))
            return;

        double dmeasure = ((Number) measure).doubleValue();

        if (!result.hasMax() || result.getMax() < dmeasure) {
            result.setMax(dmeasure);
        }
    }

    @Override
    public Object getAggrValue(Aggregation source) {
        return source.hasMin() ? source.getMin() : null;
    }

}
