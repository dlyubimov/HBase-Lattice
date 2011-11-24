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

import java.io.IOException;

import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.model.IrregularSample;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.protocodegen.Cells.Aggregation.Builder;
import com.inadco.math.aggregators.OnlineCannyAvgSummarizer;

/**
 * Canny function -based aggregation support for averages on fact streams with
 * irregular sampling.<P>
 * 
 * This function expects facts of {@link IrregularSample} type for
 * {@link #apply(Builder, Object)} updates during compilation and serialized
 * {@link OnlineCannyAvgSummarizer} states for
 * {@link #merge(Builder, Aggregation, SliceOperation)} during compilation/query
 * time.
 * 
 * @author dmitriy
 * 
 */
public class FCannyAvgSum extends FCustomFunc {

    // private double dt;
    private OnlineCannyAvgSummarizer sumBuf, sumBuf1;

    /**
     * Constructor 
     * 
     * @param name function name to register with.
     * @param ordinal
     * @param dt
     */
    public FCannyAvgSum(String name, int ordinal, double dt) {
        super(name, ordinal);
        // this.dt = dt;
        sumBuf = new OnlineCannyAvgSummarizer(dt);
        sumBuf1 = new OnlineCannyAvgSummarizer(dt);
    }

    @Override
    public void apply(Builder result, Object measureFact) {
        if (!(measureFact instanceof IrregularSample))
            return; // we don't know how to sum anything else
        IrregularSample sample = (IrregularSample) measureFact;

        sumBuf.reset();
        Object fact = sample.getFact();
        if (!(fact instanceof Number))
            return;

        double x = ((Number) fact).doubleValue();

        sumBuf.update(x, sample.getTime());

        try {
            super.saveState(result, sumBuf);
        } catch (IOException exc) {
            // should not happen .
            // otherwise, probably a bad practice.
            throw new RuntimeException(exc);
        }
    }

    @Override
    public void merge(Builder accumulator, Aggregation source, SliceOperation operation) {
        try {
            super.extractState(accumulator, sumBuf);
            super.extractState(source, sumBuf1);

            switch (operation) {
            case ADD:
                sumBuf.combine(sumBuf1);
                break;
            case COMPLEMENT:
                sumBuf.complement(sumBuf1, true);
                break;
            default:
                throw new RuntimeException("Unsupported slice operation");
            }
            super.saveState(accumulator, sumBuf);

        } catch (IOException exc) {
            // should not happen .
            // otherwise, probably a bad practice.
            throw new RuntimeException(exc);
        }

    }

    @Override
    public boolean supportsComplementScan() {
        return true;
    }

    @Override
    public Object getAggrValue(Aggregation source) {
        try {
            return extractState(source, new OnlineCannyAvgSummarizer());

        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }

    }

}
