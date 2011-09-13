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
        if (o.w > w) {
            w = o.w;
            v = o.v;
        }
        // events are just summed up
        if (t >= o.t) {
            s += pi * o.s;
            u += nu * o.u;
        } else {
            s = pi * s + o.s;
            u = nu * u + o.u;
        }
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
