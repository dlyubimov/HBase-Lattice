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
 * aggregate function of Standard (biased) deviation
 * 
 * @author dmitriy
 * 
 */
public class FStdDev extends AbstractAggregateFunc {

    private AggregateFunction varFunc;

    public FStdDev() {
        super("SD");
    }

    @Override
    public Object getAggrValue(Aggregation source) {
        initDependencies();
        Number variance = (Number) varFunc.getAggrValue(source);
        return variance == null ? null : Math.sqrt(variance.doubleValue());
    }

    private void initDependencies() {
        if (varFunc == null) {
            varFunc = parent.findFunction(FStdVar.FNAME);
            Validate.notNull(varFunc);
        }
    }

}
