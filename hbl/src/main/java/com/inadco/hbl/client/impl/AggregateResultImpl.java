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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.AggregateFunction;
import com.inadco.hbl.client.AggregateFunctionRegistry;
import com.inadco.hbl.client.AggregateResult;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

public class AggregateResultImpl implements AggregateResult {

    private Map<String, Object>       groupMembers;     // dimension members by
                                                         // dim name
    private Map<String, Aggregation>  measureAggregates;
    private AggregateFunctionRegistry afr;

    // private Map<String, AggregateFunction> functions;

    AggregateResultImpl(AggregateFunctionRegistry afr) {
        super();
        this.afr = afr;
        groupMembers = new HashMap<String, Object>();
        measureAggregates = new HashMap<String, Aggregation>();
    }

    Map<String, Object> getGroupMembers() {
        return groupMembers;
    }

    Map<String, Aggregation> getMeasureAggregates() {
        return measureAggregates;
    }

    @Override
    public double getDoubleAggregate(String measure, String functionName) {
        AggregateFunction func = afr.findFunction(functionName);
        Validate.notNull(func, "aggregate function not supported");
        Aggregation measureAggregate = measureAggregates.get(measure);
        Validate.notNull(measureAggregate, "requested measure is not in the query result");

        return func.getDoubleValue(measureAggregate);
    }

    @Override
    public Object getGroupMember(String dimensionName) {
        return groupMembers.get(dimensionName);
    }

}
