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
 * 
 * We also assume that t==0 means we have no observations (so observations
 * cannot actually start at time t_0 = 0).
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
        Validate
            .isTrue(other instanceof OnlineExpRateSummarizer, "Unable to combine with incompatible summarizer type");
        OnlineExpRateSummarizer o = (OnlineExpRateSummarizer) other;
        Validate.isTrue(o.alpha == alpha, "Unable to combine incompatible summarizers: different exponential decay.");

        if (o.t == 0) {
            // do nothing -- other summarizer was empty

        } else if (t == 0) { // we did not have observations
            w = o.w;
            s = o.s;
            t = o.t;
        } else if (t >= o.t) {
            double pi = Math.exp((o.t - t) / alpha);
            w = Math.max(w, pi * o.w + (1 - pi) * alpha);
            s += pi * o.s;
        } else {
            double pi = Math.exp((t - o.t) / alpha);
            w = Math.max(o.w, pi * w + (1 - pi) * alpha);
            s = o.s + pi * s;
            t = o.t;
        }
    }

    /**
     * warning. This works only for continuous slices that are connected
     * properly.
     * <P>
     * 
     * i.e. if you have timeline t1...tn such that t1<t2...<tn, and you had
     * samples corresponding to times t1..ti added to S1, then it will create a
     * rate for interval corresponding to ti..tn but samples x_i+1..xn which is
     * probably not what one expects exactly. The solution is of course to add
     * sample (0, ti) to the "other" summarizer and then this will return
     * expected sum of (x_1,t_1)...(x_i,t_i).
     * <P>
     * 
     * Better yet is to set explicit boundaries for slices by adding 0 samples
     * at the slice boundaries.
     * <P>
     */
    @Override
    public void complement(IrregularSamplingSummarizer other, boolean artificialStretch) {

        Validate
            .isTrue(other instanceof OnlineExpRateSummarizer, "Unable to combine with incompatible summarizer type");
        OnlineExpRateSummarizer o = (OnlineExpRateSummarizer) other;
        Validate.isTrue(o.alpha == alpha, "Unable to combine incompatible summarizers: different exponential decay.");

        Validate.isTrue(t >= o.t || artificialStretch,
                        "we are supposed to be a superset (this.t >= other.t) doesn't hold.");

        double pi;
        if (t < o.t) {
            // so apparently 'other' are newer observations and
            // we are missing some of the new observations in the
            // superset. So what we can do in this case, we can "stretch"
            // ourselves without events. Obviously, this would mean
            // we did not see what the other party saw.
            pi = Math.exp((t - o.t) / alpha);
            t = o.t;
            w += (1 - pi) * alpha;
            s = pi * s;
            pi = 1;
        } else
            pi = Math.exp((o.t - t) / alpha);

        // another problem is that we don't really know if other corresponds to
        // last events or less than last events, so we can't correct t exactly.
        // This will affect stuff like biased estimators, because from their
        // point of view, there just were no recent observations. So complements
        // are probably not for biased estimates so much.
        s -= pi * o.s;
        w -= pi * o.w;

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

    /**
     * return time of the first sample (reconstructed, approx)
     * 
     * @return
     */
    public double getT0() {
        if (t == 0)
            return 0; // no observations;
        double delta = -alpha * Math.log(1 - w / alpha);

        assert delta >= 0;

        return t - delta;
    }

    @Override
    public String toString() {
        return "OnlineExpRateSummarizer [w=" + w + ", s=" + s + ", t=" + t + ", alpha=" + alpha + "]";
    }

}
