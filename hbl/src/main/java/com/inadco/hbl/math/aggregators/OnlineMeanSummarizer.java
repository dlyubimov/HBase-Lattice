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
package com.inadco.hbl.math.aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.Validate;

/**
 * Welford summarizer with combiner.
 * 
 * @author dmitriy
 * 
 */
public class OnlineMeanSummarizer implements OnlineSummarizer<OnlineMeanSummarizer> {

    /**
     * maximum amt of history, after which it is ok start 'forgetting' the
     * numbers.
     */
    long   maxN = 100000000;

    double mean;
    double variance;
    long   n;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public OnlineMeanSummarizer(long maxN) {
        super();
        this.maxN = maxN;
    }

    public OnlineMeanSummarizer() {
        super();
    }

    public void update(double x) {
        double oldMean = mean;
        mean += (x - mean) / ++n;
        variance = (variance * (n - 1) + (x - mean) * (x - oldMean)) / n;
        // variance += (variance - (x - mean) * (x - oldMean)) / n;
        if (n > maxN)
            n = maxN;
    }

    @Override
    public void assign(OnlineMeanSummarizer other) {
        maxN = other.maxN;
        mean = other.mean;
        variance = other.variance;
        n = other.n;
    }

    @Override
    public void reset() {
        mean = 0;
        variance = 0;
        n = 0;
    }

    @Override
    public double getValue() {
        return getMean();
    }

    public double getMean() {
        return mean;
    }

    public double getVariance() {
        return variance;
    }

    @Override
    public void combine(OnlineMeanSummarizer other) {
        // todo: test variance combining.
        double delta = mean - other.mean;
        variance = delta * delta * n * other.n / (n + other.n) + variance * n + other.variance * other.n;

        mean = (mean * n + other.mean * other.n) / (n + other.n);
        n += other.n;
        if (n > maxN)
            n = maxN;
    }

    @Override
    public void complement(OnlineMeanSummarizer other, boolean artificialStretch) {
        // FIXME: this should be possible here, shouldn't it?
        throw new UnsupportedOperationException();

    }

    @Override
    public void combineBinomialOnes(OnlineMeanSummarizer positiveHistory) {

        // i don't think we can combine variance this way reliably.
        Validate.isTrue(n >= positiveHistory.n);
        n -= positiveHistory.n;
        combine(positiveHistory);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mean = in.readDouble();
        variance = in.readDouble();
        n = in.readLong();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(mean);
        out.writeDouble(variance);
        out.writeDouble(n);

    }

    @Override
    public String toString() {
        return "OnlineMeanSummarizer [maxN=" + maxN + ", mean=" + mean + ", variance=" + variance + ", n=" + n + "]";
    }

}
