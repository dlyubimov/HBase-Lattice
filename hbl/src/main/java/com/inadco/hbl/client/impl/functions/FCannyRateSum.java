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
import com.inadco.math.aggregators.OnlineCannyRateSummarizer;

/**
 * Canny function -based aggregation support for rates on fact streams with
 * irregular sampling.
 * <P>
 * 
 * This function expects facts of {@link IrregularSample} type for
 * {@link #apply(Builder, Object)} updates during compilation and serialized
 * {@link OnlineCannyAvgSummarizer} states for
 * {@link #merge(Builder, Aggregation, SliceOperation)} during compilation/query
 * time.
 * <P>
 * 
 * <b>Warning</b>: complement is fundamentally difficult with rate summarizer, 
 * so we report complement as unavailable functionality for rates.<P>
 * 
 * 
 * @author dmitriy
 * 
 */
public class FCannyRateSum extends FCustomFunc {

    // private double dt;
    private OnlineCannyRateSummarizer sumBuf, sumBuf1;

    /**
     * Constructor
     * 
     * @param name
     *            function name to register with.
     * @param ordinal
     * @param dt
     */
    public FCannyRateSum(String name, int ordinal, double dt) {
        super(name, ordinal);
        // this.dt = dt;
        sumBuf = new OnlineCannyRateSummarizer(dt);
        sumBuf1 = new OnlineCannyRateSummarizer(dt);
    }

    @Override
    public void apply(Builder result, Object measureFact) {
        if (!(measureFact instanceof IrregularSample))
            return; // we don't know how to sum anything else
        IrregularSample sample = (IrregularSample) measureFact;

        Object fact = sample.getFact();
        if (!(fact instanceof Number))
            return;

        double x = ((Number) fact).doubleValue();

        try {

            OnlineCannyRateSummarizer s = super.extractState(result, sumBuf);
            if (s == null) {
                sumBuf.reset();
                s = sumBuf;
            }
            s.update(x, sample.getTime());

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
            OnlineCannyRateSummarizer s1 = super.extractState(accumulator, sumBuf);
            OnlineCannyRateSummarizer s2 = super.extractState(source, sumBuf1);

            switch (operation) {
            case ADD:
                if (s1 != null && s2 != null)
                    s1.combine(s2);
                else if (s1 == null)
                    s1 = s2;
                break;
            case COMPLEMENT:
                if (s2 != null && s1 != null)
                    s1.complement(s2, true);
                break;
            default:
                throw new RuntimeException("Unsupported slice operation");
            }
            if (s1 != null)
                super.saveState(accumulator, s1);

        } catch (IOException exc) {
            // should not happen .
            // otherwise, probably a bad practice.
            throw new RuntimeException(exc);
        }

    }

    @Override
    public boolean supportsComplementScan() {
        return false;
    }

    @Override
    public Object getAggrValue(Aggregation source) {
        try {
            /*
             * since we don't have control over result object lifecycle in this
             * case, we'd rather create a new summarizer for each incoming
             * request.
             */
            return extractState(source, new OnlineCannyRateSummarizer());
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }

    }

}
