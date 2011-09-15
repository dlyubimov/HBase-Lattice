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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.AggregateQuery;
import com.inadco.hbl.client.AggregateResult;

public class AggregateQueryImpl implements AggregateQuery {

    private Cube                 cube;
    /**
     * dim name -> range slice requested
     */
    private Map<String, Range[]> dimSlices = new HashMap<String, Range[]>();
    private Set<String>          measures  = new HashSet<String>();

    public AggregateQueryImpl(Cube cube) {
        super();
        this.cube = cube;
    }

    @Override
    public AggregateQuery addMeasure(String measure) {
        Validate.notNull(measure);
        measures.add(measure);
        return this;
    }

    @Override
    public AggregateQuery addClosedSlice(String dimension, Object leftBound, Object rightBound) {
        return addSlice(dimension, leftBound, false, rightBound, false);
    }

    @Override
    public AggregateQuery addOpenSlice(String dimension, Object leftBound, Object rightBound) {
        return addSlice(dimension, leftBound, true, rightBound, true);
    }

    @Override
    public AggregateQuery addHalfOpenSlice(String dimension, Object leftBound, Object rightBound) {
        return addSlice(dimension, leftBound, false, rightBound, true);
    }

    @Override
    public AggregateQuery addSlice(String dimension,
                                   Object leftBound,
                                   boolean leftOpen,
                                   Object rightBound,
                                   boolean rightOpen) {
        Validate.notNull(dimension);
        if (leftBound == null && rightBound == null) {
            dimSlices.remove(dimension);
            return this;
        }
        Dimension dim = cube.getDimensions().get(dimension);
        Validate.notNull(dim, "specified dimension is not in this model");

        // TODO: to be cntd.
        return this;
    }

    @Override
    public AggregateResult execute() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
