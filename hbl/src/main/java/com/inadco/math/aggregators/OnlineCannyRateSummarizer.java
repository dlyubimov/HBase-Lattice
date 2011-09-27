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
 * Rate summarizer with Canny filter-weighted history.
 * <P>
 * 
 * (Canny's filter is just a difference of 2 exponents and we assert f'(0)=0 and
 * f(0)=1 on top of it)
 * <P>
 * 
 * @author Dmitriy
 * 
 */
public class OnlineCannyRateSummarizer extends OnlineCannyAvgSummarizer {

    public OnlineCannyRateSummarizer() {
        super();
    }

    public OnlineCannyRateSummarizer(double dt, double m, double k) {
        super(dt, m, k);
    }

    public OnlineCannyRateSummarizer(double dt) {
        super(dt);
    }

    @Override
    public void combine(IrregularSamplingSummarizer other) {
        Validate.isTrue(other instanceof OnlineCannyRateSummarizer);
        OnlineCannyRateSummarizer o = (OnlineCannyRateSummarizer) other;
        if (t == 0) {
            // we don't have any history
            assign(o);
            return;
        }
        if (o.t == 0) {
            // the other guy doesn't have any history
            return;
        }
        double delta = Math.abs(t - o.t);
        double pi = Math.exp(-delta / alpha);
        double nu = Math.exp(-k * delta / alpha / (k - 1));
        // hist len -- take whichever longer
        // if (o.w > w) {
        // w = o.w;
        // v = o.v;
        // }
        // events are just summed up
        if (t >= o.t) {
            s += pi * o.s;
            u += nu * o.u;
            double oHist = pi * o.w + (1 - pi) * alpha;
            if (oHist > w) {
                w = oHist;
                v = nu * o.v + alpha * (k - 1) * (1 - nu) / k;
            }
        } else {
            s = pi * s + o.s;
            u = nu * u + o.u;
            double tHist = pi * w + (1 - pi) * alpha;
            if (tHist > o.w) {
                w = tHist;
                v = nu * v + alpha * (k - 1) * (1 - nu) / k;
            } else {
                w = o.w;
                v = o.v;
            }
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
        Validate.isTrue(other instanceof OnlineCannyRateSummarizer);
        OnlineCannyRateSummarizer o = (OnlineCannyRateSummarizer) other;
        Validate.isTrue(o.alpha == alpha, "Unable to combine incompatible summarizers: different exponential decay.");
        Validate.isTrue(o.k == k, "Unable to combine incompatible summarizers: different k parameter in Canny filter.");
        Validate.isTrue(t >= o.t || artificialStretch,
                        "we are supposed to be a superset (this.t >= other.t) doesn't hold.");

        double pi, nu;
        if (t < o.t) {
            // so apparently 'other' are newer observations and
            // we are missing some of the new observations in the
            // superset. So what we can do in this case, we can "stretch"
            // ourselves without events. Obviously, this would mean
            // we did not see what the other party saw.
            double delta = o.t - t;
            pi = Math.exp(-delta / alpha);
            nu = Math.exp(-k * delta / alpha / (k - 1));

            t = o.t;
            w *= (1 - pi) * alpha;
            v *= alpha * (k - 1) * (1 - nu) / k;
            s *= pi;
            u *= nu;
            pi = nu = 1;
        } else {
            double delta = t - o.t;
            pi = Math.exp(-delta / alpha);
            nu = Math.exp(-k * delta / alpha / (k - 1));
        }

        // another problem is that we don't really know if other corresponds to
        // last events or less than last events, so we can't correct t exactly.
        // This will affect stuff like biased estimators, because from their
        // point of view, there just were no recent observations. So complements
        // are probably not for biased estimates so much.
        s -= pi * o.s;
        u -= nu * o.u;
        w -= pi * o.w;
        v -= nu * o.v;

    }

    @Override
    public void combineBinomialOnes(IrregularSamplingSummarizer positiveHistory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getValueNow(double tNow) {
        if (tNow < t)
            tNow = t;
        return addFuture(0, tNow, false);
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
    protected double addFuture(double x, double t, boolean doUpdate) {
        double pi = this.t == 0 ? 1 : Math.exp((this.t - t) / alpha);
        double nu = this.t == 0 ? 1 : Math.exp(k * (this.t - t) / alpha / (k - 1));
        double w = pi * this.w + alpha * (1 - pi);
        double v = nu * this.v + alpha * (k - 1) * (1 - nu) / k;
        double s = x + pi * this.s;
        double u = x + nu * this.u;

        if (doUpdate) {
            this.w = w;
            this.v = v;
            this.s = s;
            this.u = u;
            this.t = t;
            return getValue();
        }
        return (k * s - (k - 1) * u) / (k * w - (k - 1) * v);
    }

    @Override
    protected double updatePast(double x, double t) {
        double pi = Math.exp(-(this.t - t) / alpha);
        double nu = Math.exp(-k * (this.t - t) / alpha / (k - 1));
        s += pi * x;
        u += nu * x;
        double phist = alpha * (1 - pi);
        if (phist > w) {
            // this update event happened before
            // start of our hist so we need
            // to extend our history to reflect the
            // actual start time.
            w = phist;
            v = alpha * (k - 1) * (1 - nu) / k;
        }
        return getValue();
    }

}
