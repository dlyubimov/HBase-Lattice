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
package com.inadco.math.aggregators;

import org.apache.commons.lang.Validate;

/**
 * Ongoing event rate summarizer. x (as in {@link #update(double, double)} call)
 * corresponds to number of events .
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class OnlineExpRateSummarizer extends OnlineExpAvgSummarizer {

    public OnlineExpRateSummarizer() {
        super();
    }

    public OnlineExpRateSummarizer(double dt, double m) {
        super(dt, m);
    }

    public OnlineExpRateSummarizer(double dt) {
        super(dt);
    }

    public OnlineExpRateSummarizer(OnlineExpAvgSummarizer other) {
        super(other);
    }

    /**
     * return rate with accounting for amt of time that happend since last
     * measurement.
     * <P>
     * 
     * @param t
     *            the "now" time.
     * @return exponentially weighted rate.
     */
    public double rNow(double t) {
        if (t < this.t)
            // info about past is already lost, can't exclude events that
            // happened after that.
            // will show latest rate instead.
            t = this.t;
        return addFuture(0, t, false);
    }

    /**
     * get rate as of the time of most recent update (time-wise, not order
     * wise). To also account for the time passed since then, use
     * {@link #rNow(double)}.
     * <P>
     * 
     * @return the rate.
     */
    public double getRate() {
        return getAvg();
    }

    /**
     * Combine state of this summarizer with a state of another summarizer.
     * Combined rate would reflect combined history of independent events in
     * time.
     * <P>
     * 
     */
    @Override
    public void combine(IrregularSamplingSummarizer other) {
        Validate.isTrue(other instanceof OnlineExpRateSummarizer,
                        "Unable to combine with incompatible summarizer type");
        OnlineExpRateSummarizer o = (OnlineExpRateSummarizer) other;
        if (o.alpha != alpha)
            throw new IllegalArgumentException(
                "Unable to combine incompatible summarizers: different exponential decay.");

        if (o.t == 0) {
            // do nothing -- other summarizer was empty

        } else if (t == 0) { // we did not have observations
            w = o.w;
            s = o.s;
            t = o.t;
        } else if (t >= o.t) {
            double pi = Math.exp((o.t - t) / alpha);
            w = Math.max(w, pi * o.w);
            s += pi * o.s;
        } else {
            double pi = Math.exp((t - o.t) / alpha);
            w = Math.max(o.w, pi * w);
            s = o.s + pi * s;
            t = o.t;
        }
    }

    @Override
    protected double addFuture(double x, double t, boolean doUpdate) {
        double delta = t - this.t;
        double pi = this.t == 0 ? 1 : Math.exp(-delta / alpha);
        double w = pi * this.w + (1 - pi) * alpha;
        double s = x + pi * this.s;
        if (doUpdate) {
            this.w = w;
            this.s = s;
            this.t = t;
        }
        return s / w;
    }

    @Override
    protected double updatePast(double x, double t) {
        double delta = this.t - t;
        double pi = Math.exp(-delta / alpha);
        w = Math.max(w, (1 - pi) * alpha);
        s += pi * x;
        return s / w;
    }

}
