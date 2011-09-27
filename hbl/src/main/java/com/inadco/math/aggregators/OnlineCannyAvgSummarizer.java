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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.Validate;

/**
 * John Canny's summarizer. see exponentital summarizers TDD for definitions.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class OnlineCannyAvgSummarizer implements IrregularSamplingSummarizer {

    public static final double DEFAULT_MARGIN = 0.01;
    public static final double DEFAULT_K      = 4d;

    protected double           alpha, k;
    protected double           w, u, s, v;
    protected double           t;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public OnlineCannyAvgSummarizer() {
        super();
    }

    public OnlineCannyAvgSummarizer(OnlineCannyAvgSummarizer other) {
        super();
        assign(other);
    }

    public OnlineCannyAvgSummarizer(double dt, double m, double k) {
        super();
        Validate.isTrue(dt > 0);
        Validate.isTrue(m > 0);
        Validate.isTrue(k > 1);
        alpha = -dt / Math.log(m / k);
        this.k = k;

    }

    public OnlineCannyAvgSummarizer(double dt) {
        this(dt, DEFAULT_MARGIN, DEFAULT_K);
    }

    @Override
    public void reset() {
        w = u = s = v = t = 0;

    }

    @Override
    public void assign(IrregularSamplingSummarizer other) {
        Validate.isTrue(other instanceof OnlineCannyAvgSummarizer);

        OnlineCannyAvgSummarizer o = (OnlineCannyAvgSummarizer) other;
        w = o.w;
        u = o.u;
        s = o.s;
        v = o.v;
        alpha = o.alpha;
        k = o.k;
        t = o.t;
    }

    @Override
    public void combine(IrregularSamplingSummarizer other) {
        Validate.isTrue(other instanceof OnlineCannyAvgSummarizer);
        OnlineCannyAvgSummarizer o = (OnlineCannyAvgSummarizer) other;
        Validate.isTrue(o.alpha == alpha, "Unable to combine incompatible summarizers: different exponential decay.");
        Validate.isTrue(o.k == k, "Unable to combine incompatible summarizers: different k parameter in Canny filter.");

        if (o.t == 0) {
            // do nothing -- other summarizer was empty

        } else if (t == 0) // we did not have observations
            assign(other);
        else {

            double delta = Math.abs(t - o.t);
            double pi = Math.exp(-delta / alpha);
            double nu = Math.exp(-k * delta / alpha / (k - 1));

            if (t >= o.t) {
                w += pi * o.w;
                v += nu * o.v;
                s += pi * o.s;
                u += nu * o.u;
            } else {
                w = o.w + pi * w;
                v = o.v + nu * v;
                s = o.s + pi * s;
                u = o.u + nu * u;
                t = o.t;
            }
        }

    }

    @Override
    public void complement(IrregularSamplingSummarizer other, boolean artificialStretch) {

        Validate.isTrue(other instanceof OnlineCannyAvgSummarizer);
        OnlineCannyAvgSummarizer o = (OnlineCannyAvgSummarizer) other;
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
            w *= pi;
            v *= nu;
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

    /**
     * TODO: unit-test this!
     */
    @Override
    public void combineBinomialOnes(IrregularSamplingSummarizer positiveHistory) {
        Validate.isTrue(positiveHistory instanceof OnlineCannyAvgSummarizer);
        OnlineCannyAvgSummarizer o = (OnlineCannyAvgSummarizer) positiveHistory;
        Validate.isTrue(o.alpha == alpha, "Unable to combine incompatible summarizers: different exponential decay.");
        Validate.isTrue(o.k == k, "Unable to combine incompatible summarizers: different k parameter in Canny filter.");
        if (o.t == 0)
            return; // we have all the history.
        // oops! empty negative history but non-empty positive history? try
        // to use just positive history,
        // but here it is an error!
        Validate
            .isTrue(t != 0,
                    "empty negative history and non-empty positive history cannot be combined in a meaningful way.");

        double delta = Math.abs(t - o.t);
        double pi = Math.exp(-delta / alpha);
        double nu = Math.exp(-k * delta / alpha / (k - 1));

        if (t >= o.t) {
            s += o.s * pi;
            u += o.u * nu;
        } else {
            t = o.t;
            s = pi * s + o.s;
            u = nu * u + o.u;
        }

    }

    @Override
    public double update(double x, double t) {
        if (t < this.t)
            return updatePast(x, t);
        return addFuture(x, t, true);
    }

    @Override
    public double getValue() {
        return w == 0 ? 0 : (k * s - (k - 1) * u) / (k * w - (k - 1) * v);
    }

    @Override
    public double getValueNow(double tNow) {
        // it doesn't matter how much time has passed,
        // it actually won't change the average .
        return getValue();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        alpha = in.readDouble();
        k = in.readDouble();
        w = in.readDouble();
        v = in.readDouble();
        s = in.readDouble();
        u = in.readDouble();
        t = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(alpha);
        out.writeDouble(k);
        out.writeDouble(w);
        out.writeDouble(v);
        out.writeDouble(s);
        out.writeDouble(u);
        out.writeDouble(t);
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
            return getValue();
        }
        return (k * s - (k - 1) * u) / (k * w - (k - 1) * v);
    }

    protected double updatePast(double x, double t) {
        double pi = Math.exp((t - this.t) / alpha);
        double nu = Math.exp(k * (t - this.t) / alpha / (k - 1));
        w += pi;
        v += nu;
        s += pi * x;
        u += nu * x;
        return getValue();
    }

}
