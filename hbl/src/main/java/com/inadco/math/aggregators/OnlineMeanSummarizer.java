package com.inadco.math.aggregators;

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

}
