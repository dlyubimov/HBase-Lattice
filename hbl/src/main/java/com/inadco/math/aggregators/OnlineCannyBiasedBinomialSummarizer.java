package com.inadco.math.aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Canny summarizer's version of biased binomial estimator.
 * 
 * @see OnlineExpBiasedBinomialSummarizer
 * 
 * @author dmitriy
 * 
 */
public class OnlineCannyBiasedBinomialSummarizer extends OnlineCannyAvgSummarizer {

    public static double DEFAULT_EPSILON = 0.3d;

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
    public OnlineCannyBiasedBinomialSummarizer(OnlineCannyAvgSummarizer state, double p0, double epsilon) {
        super(state);
        
        // TODO: this estimate is still based on an exponent behavior, not on Canny's difference of exponents, so it will 
        // overestimate bposneg. It looks like it is hard to actually estimate CannyFunction^-1(epsilon), so i'll take 
        // my chances with exponent. Exponent estimate will reduce bias components, so actual bias behavior will be less 
        // aggressive than needed. So i will reduce default epsilon instead.
        
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
    public OnlineCannyBiasedBinomialSummarizer(OnlineCannyAvgSummarizer state, double p0) {
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
     * @param k
     *            Canny's k parameter
     */
    public OnlineCannyBiasedBinomialSummarizer(double p0, double epsilon, double dt, double m, double k) {
        super(dt, m, k);

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
    public OnlineCannyBiasedBinomialSummarizer(double p0, double dt) {
        this(p0, DEFAULT_EPSILON, dt, DEFAULT_MARGIN, DEFAULT_K);
    }

    /**
     * with reasonable defaults (1 wk of history assuming time is in ms )
     */
    public OnlineCannyBiasedBinomialSummarizer() {
        this(0.5d, 7 * 24 * 3600 * 1000);
    }

    @Override
    public double getValue() {
        return super.getValue();
    }

    @Override
    public double getValueNow(double tNow) {
        return pnow(tNow);
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
        OnlineExpBiasedBinomialSummarizer oth = (OnlineExpBiasedBinomialSummarizer) other;
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
        double nu = this.t == 0 ? 0 : Math.exp(k * (this.t - t) / alpha / (k - 1));
        double w = pi * this.w;
        double v = nu * this.v;
        double s = x + pi * this.s;
        double u = x + nu * this.u;

        if (doUpdate) {
            this.w = ++w;
            this.v = ++v;
            this.s = s;
            this.u = u;
            this.t = t;
            return (bpos + k * this.s - (k - 1) * this.u) / (bpos + bneg + k * this.w - (k - 1) * this.v);
        }
        return (bpos + k * s - (k - 1) * u) / (bpos + bneg + k * w - (k - 1) * v);
    }

    @Override
    protected double updatePast(double x, double t) {
        super.updatePast(x, t);
        // this affects only biased estimate returned.
        return (bpos + k * s - (k - 1) * u) / (bpos + bneg + k * w - (k - 1) * v);
    }
}
