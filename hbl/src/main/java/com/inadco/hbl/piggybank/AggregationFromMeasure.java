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
package com.inadco.hbl.piggybank;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.commons.lang.Validate;
import org.apache.pig.PigException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.inadco.hbl.api.AggregateFunctionRegistry;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.model.SimpleAggregateFunctionRegistry;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

/**
 * 
 * Pig function that takes the measure value, and uses measure to translate it
 * to double first and then {@link SimpleAggregateFunctionRegistry} to produce
 * final {@link Aggregation}.
 * <P>
 * 
 * meausure name and model must be configured thru Pig's DEFINE.
 * <P>
 * 
 * The result is byte array containing serialized {@link Aggregation} measure.
 * 
 * @author dmitriy
 * 
 */
public class AggregationFromMeasure extends BaseFunc<DataByteArray> {

    private AggregateFunctionRegistry afr;
    private String                    measureName;

    public AggregationFromMeasure(String measureName, String encodedModel) throws PigException {
        super(encodedModel);
        this.measureName = measureName;
        this.afr = super.cube.getAggregateFunctionRegistry();
    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        Object measureKey = input.get(0);
        Measure m = measureKey == null ? null : cube.getMeasures().get(measureName);
        Validate.notNull(m, "no measure passed/found");
        Object d = m.compiler2Fact(input.get(1));
        // we don't measures to evaluate to nulls to simplify null issues .
        if (d == null)
            return null;

        Aggregation.Builder aggrb = Aggregation.newBuilder();
        afr.applyAll(aggrb, d);
        return new DataByteArray(aggrb.build().toByteArray());

    }

    @Override
    public Type getReturnType() {
        return DataByteArray.class;
    }

}
