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

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.AggregateFunction;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

/**
 * aggregate function of Standard (biased) variance
 * 
 * @author dmitriy
 * 
 */
public class FStdVar extends AbstractAggregateFunc {

    public static final String FNAME = "VAR";

    private AggregateFunction  countFunc;
    private AggregateFunction  avgFunc;
    private AggregateFunction  sumSqFunc;

    public FStdVar() {
        super(FNAME);
    }

    @Override
    public Object getAggrValue(Aggregation source) {
        /*
         * undefined on the empty group
         */
        if (source == null)
            return null;

        initDependencies();
        Number avg = (Number) avgFunc.getAggrValue(source);
        if (avg == null)
            return null;
        Number sumSq = (Number) sumSqFunc.getAggrValue(source);
        if (sumSq == null)
            return null;

        Number cnt = (Number) countFunc.getAggrValue(source);
        if (cnt == null)
            return null; // should not happen

        double dsumSq = sumSq.doubleValue() / cnt.doubleValue();
        double davg = avg.doubleValue();
        return dsumSq - davg * davg;
    }

    private void initDependencies() {
        if (countFunc == null) {
            countFunc = parent.findFunction(FCount.FNAME);
            avgFunc = parent.findFunction(FSum.FNAME);
            sumSqFunc = parent.findFunction(FSumSq.FNAME);
            Validate.notNull(countFunc);
            Validate.notNull(avgFunc);
            Validate.notNull(sumSqFunc);
        }
    }
}
