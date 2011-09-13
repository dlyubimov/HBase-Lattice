package com.inadco.math.aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * see section 2.2.0 of FAS 2.0 Sale expectaiton.pdf for math details of
 * exponentially weighted biased probability estimator with irregular sampling
 * <P>
 * 
 * This class is introducing conjugate prior bias for binomial summarizer (i.e.
 * x must be 1 or 0, perhaps some observations could be 'partial' but it is not
 * average anymore!)
 * <P>
 * There's also unbiased version somehwere in mahout (MAHOUT-634 I think)
 * 
 * @author dmitriy
 * 
 */
public class OnlineExpBiasedBinomialSummarizer extends OnlineExpAvgSummarizer {

    public static double DEFAULT_EPSILON = 0.5d;

    // parameters
    protected double     bpos, bneg;

    /**
     * take state from average summarizer and turn it into biased summarizer
     * 
     * @param state
     * @param p0
     *            initial bias
     * @param epsilon
     *            epsilon (amt of most recent significant history, exponentially
     *            weighted) 0..1
     */
    public OnlineExpBiasedBinomialSummarizer(OnlineExpAvgSummarizer state,
                                                              double p0,
                                                              double epsilon) {
        super(state);
        double bposneg = 2 * (epsilon - 1) / Math.log(epsilon);
        bpos = bposneg * p0;
        bneg = bposneg * (1 - p0);
    }

    /**
     * take state from average summarizer and turn it into biased summarizer
     * 
     * @param state
     * @param p0
     *            initial bias
     */
    public OnlineExpBiasedBinomialSummarizer(OnlineExpAvgSummarizer state, double p0) {
        this(state, p0, DEFAULT_EPSILON);
    }

    /**
     * @param p0
     *            P_0 (null binomial hypothesis, 0..1
     * @param epsilon
     *            epsilon (amt of most recent significant history, exponentially
     *            weighted) 0..1
     * @param dt
     *            time period for phaseout
     * @param m
     *            phaseout margin (amount of exponent still left after dt has
     *            passed)
     */
    public OnlineExpBiasedBinomialSummarizer(double p0, double epsilon, double dt, double m) {
        super(dt, m);

        resetBias(p0, epsilon);
    }

    /**
     * with reasonable defaults
     * 
     * @param p0
     *            P_0 (null binomial hypothesis) 0..1
     * @param dt
     *            amount of history, time-wise
     */
    public OnlineExpBiasedBinomialSummarizer(double p0, double dt) {
        this(p0, DEFAULT_EPSILON, dt, DEFAULT_HISTORY_MARGIN);
    }

    /**
     * with reasonable defaults (1 wk of history assuming time is in ms )
     */
    public OnlineExpBiasedBinomialSummarizer() {
        this(0.5d, 7 * 24 * 3600 * 1000);
    }

    @Override
    public double getValue() {
        return getAvg();
    }

    @Override
    public double getValueNow(double tNow) {
        return pnow(tNow);
    }

    @Override
    public double getAvg() {
        // return pnow(System.currentTimeMillis());
        return super.getAvg();
    }

    public double pnow(double t) {
        return addFuture(0, t, false);
    }

    /**
     * Set new bias parameter calculations
     * 
     * @param p0
     *            new bias to start with
     * @param epsilon
     *            new epsilon
     */
    public void resetBias(double p0, double epsilon) {
        double bposneg = 2 * (epsilon - 1) / Math.log(epsilon);
        bpos = bposneg * p0;
        bneg = bposneg * (1 - p0);
    }

    public void resetBias(double p0) {
        resetBias(p0, DEFAULT_EPSILON);
    }

    @Override
    public void combine(IrregularSamplingSummarizer other) {
        if (!(other instanceof OnlineExpBiasedBinomialSummarizer))
            throw new IllegalArgumentException("attempt to combine an incompatible summarizer");
        OnlineExpBiasedBinomialSummarizer oth =
            (OnlineExpBiasedBinomialSummarizer) other;
        if (bneg != oth.bneg || bpos != oth.bpos)
            throw new IllegalArgumentException(
                "attempt to combine an incompatible summarizer with a differently preset bias");
        super.combine(other);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        bpos = in.readDouble();
        bneg = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeDouble(bpos);
        out.writeDouble(bneg);

    }

    protected double addFuture(double x, double t, boolean doUpdate) {
        double pi = this.t == 0 ? 0 : Math.exp((this.t - t) / alpha);
        double w = pi * this.w;
        double s = x + pi * this.s;
        if (doUpdate) {
            this.w = ++w;
            this.s = s;
            this.t = t;
            return (bpos + this.s) / (bpos + bneg + this.w);
        }
        return (bpos + s) / (bpos + bneg + w);
    }

    @Override
    protected double updatePast(double x, double t) {
        super.updatePast(x, t);
        return (bpos + s) / (bpos + bneg + w);
    }

}
