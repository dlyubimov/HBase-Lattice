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
        double piArg = -delta / alpha;
        double nuk = (k - 1) / k;

        double pi = Math.exp(piArg);
        double nu = Math.exp(piArg / nuk);
        // hist len -- take whichever longer
        // if (o.w > w) {
        // w = o.w;
        // v = o.v;
        // }
        // events are just summed up
        if (t >= o.t) {
            s += pi * o.s;
            u += nu * o.u;
            double oHist = pi * o.w + alpha * (1 - pi);
            if (oHist > w) {
                w = oHist;
                v = nu * o.v + alpha * nuk * (1 - nu);
            }
        } else {
            s = pi * s + o.s;
            u = nu * u + o.u;
            double tHist = pi * w + alpha * (1 - pi);
            if (tHist > o.w) {
                w = tHist;
                v = nu * v + alpha * nuk * (1 - nu);
            } else {
                w = o.w;
                v = o.v;
            }
        }
    }

    /**
     * WARNING: time complementation is really screwed up here. It is
     * fundamentally difficult to complement time because we don't know what the
     * new time must be (last event in the complement sequence of events). This
     * information is fundamentally unknown with this technique, it seems.
     * 
     * 
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
            double piArg = -delta / alpha;
            double nuk = (k - 1) / k;
            pi = Math.exp(piArg);
            nu = Math.exp(piArg / nuk);

            t = o.t;
            w = pi * w + alpha * (1 - pi);
            v = nu * v + alpha * nuk * (1 - nu);
            s *= pi;
            u *= nu;
            pi = nu = 1;
        } else {
            double delta = t - o.t;
            double piArg = -delta / alpha;
            pi = Math.exp(piArg);
            nu = Math.exp(piArg * k / (k - 1));
        }

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
        // // FIXME:
        // throw new UnsupportedOperationException();
        return t + alpha * Math.log(1 - w / alpha);
    }

    @Override
    protected double addFuture(double x, double t, boolean doUpdate) {
        double piArg = (this.t - t) / alpha;
        double nuk = (k - 1) / k;
        double pi = this.t == 0 ? 1 : Math.exp(piArg);
        double nu = this.t == 0 ? 1 : Math.exp(piArg / nuk);
        double w = pi * this.w + alpha * (1 - pi);
        double v = nu * this.v + nuk * alpha * (1 - nu);
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
        double piArg = (t - this.t) / alpha;
        double nuk = (k - 1) / k;
        double pi = Math.exp(piArg);
        double nu = Math.exp(piArg / nuk);
        s += pi * x;
        u += nu * x;
        double phist = alpha * (1 - pi);
        if (phist > w) {
            // this update event happened before
            // start of our hist so we need
            // to extend our history to reflect the
            // actual start time.
            w = phist;
            v = nuk * alpha * (1 - nu);
        }
        return getValue();
    }

    @Override
    public String toString() {
        return "OnlineCannyRateSummarizer [alpha=" + alpha + ", k=" + k + ", w=" + w + ", u=" + u + ", s=" + s + ", v="
            + v + ", t=" + t + "]";
    }

}
