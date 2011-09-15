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
package com.inadco.hbl.client.impl;

import java.util.Map;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.client.AggregateResult;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.scanner.AggregateFunction;

public class AggregateResultImpl implements AggregateResult {

    private Map<String, Aggregation>       measureAggregates;
    private Map<String, AggregateFunction> functions;

    AggregateResultImpl(Map<String, Aggregation> measureAggregates, Map<String, AggregateFunction> functions) {
        super();
        this.measureAggregates = measureAggregates;
        this.functions = functions;
    }

    @Override
    public double getDoubleAggregate(String measure, String functionName) {
        AggregateFunction func = functions.get(functionName);
        Validate.notNull(func, "aggregate function not supported");
        Aggregation measureAggregate = measureAggregates.get(measure);
        Validate.notNull(measureAggregate, "requested measure is not in the query result");

        return func.getDoubleValue(measureAggregate);
    }

}
