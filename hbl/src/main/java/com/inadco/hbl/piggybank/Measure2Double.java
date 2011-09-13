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
import org.apache.pig.data.Tuple;

import com.inadco.hbl.api.Measure;

/**
 * @deprecated 
 * @author dmitriy
 *
 */
public class Measure2Double extends BaseFunc<Double> {

    public Measure2Double(String encodedModel) {
        super(encodedModel);
        // measure = cube.getMeasures().get(measureName);
        // Validate.notNull(measure,
        // String.format("no measure %s found in the cube model", measureName));

    }

    @Override
    public Double exec(Tuple input) throws IOException {
        Object measureKey = input.get(0);
        Measure m = measureKey == null ? null : cube.getMeasures().get(measureKey);
        Validate.notNull(m, "no measure passed/found");
        Double d = m.asDouble(input.get(1));
        // we don't measures to evaluate to nulls to simplify null issues . 
        return d == null ? 0.0 : d;
    }

    @Override
    public Type getReturnType() {
        return Double.class;
    }

}
