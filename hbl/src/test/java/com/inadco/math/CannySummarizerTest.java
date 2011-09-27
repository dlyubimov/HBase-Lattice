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
package com.inadco.math;

import java.util.Iterator;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.inadco.hbl.util.IOUtil;
import com.inadco.math.aggregators.OnlineCannyAvgSummarizer;
import com.inadco.math.aggregators.OnlineCannyRateSummarizer;

/**
 * Canny's filter test
 * 
 * @author dmitriy
 * 
 */
public class CannySummarizerTest {
    private static final boolean      DEBUG      = false;

    // private static final double BIAS = 0.75;
    // private static final double CONVERGED = 0.25;
    private static final double       HISTORYLEN = 600000;      // 10
    // MIN
    private static final int          N          = 300;
    private static final double       PREC       = 1E-3;

    private OnlineCannyAvgSummarizer  cs         = new OnlineCannyAvgSummarizer(HISTORYLEN),
        cs1 = new OnlineCannyAvgSummarizer(HISTORYLEN), cs2 = new OnlineCannyAvgSummarizer(HISTORYLEN), combined1,
        combined2, complement1, complement2;

    private OnlineCannyRateSummarizer cr         = new OnlineCannyRateSummarizer(HISTORYLEN),
        cr1 = new OnlineCannyRateSummarizer(HISTORYLEN), cr2 = new OnlineCannyRateSummarizer(HISTORYLEN), rcombined1,
        rcombined2, rcomplement1, rcomplement2;

    private double                    t;
    private Random                    rnd        = new Random();
    private int                       n;

    @Test(dataProvider = "dataprov", enabled = !DEBUG)
    public void testCannyAvg(double t, double x) {
        cs.update(x, t);

        if (rnd.nextDouble() > 0.5)
            cs1.update(x, t);
        else
            cs2.update(x, t);

//        System.out.printf("cannyAvg: %.4f\n", cs.getValue());

    }

    @Test(dataProvider = "dataprov1", enabled = !DEBUG)
    public void testCannyAvg1(double t, double x) {
        cs.update(x, t);

        if (rnd.nextDouble() > 0.5)
            cs1.update(x, t);
        else
            cs2.update(x, t);

//        System.out.printf("cannyAvg1: %.4f\n", cs.getValue());

    }

    private OnlineCannyRateSummarizer currSplit = null;

    @Test(dataProvider = "rateprov", enabled = true)
    public void testCannyRate(double t, double x) {
        double r = cr.update(x, t);

        System.out.printf("ratesum: %.4e\n", r);

        // in order to test complementing the rates,
        // we need to ensure splits are continuous.
        // complement doesn't work with anything else.

        if (currSplit == null)
            currSplit = cr1;
        else if (currSplit == cr1 && rnd.nextDouble() < 0.01) {
            currSplit = cr2;
            // in order for test to work, we have to
            // "pull" amount of time in rs1 so that it connects
            // to the start point of rs2, so
            cr1.update(0, t);
            // of course it is not going to be the case in the
            // in projections, unless we explicitly update the rate
            // summarizers there with hierarchy boundaries!
        }

        currSplit.update(x, t);

    }

    @Test(dataProvider = "rateprov1", dependsOnMethods = "testCannyRateCombineAndSplit", enabled = !DEBUG)
    public void testCannyRate1(double t, double x) {
        cr.update(x, t);

        if (rnd.nextDouble() > 0.5)
            cr1.update(x, t);
        else
            cr2.update(x, t);
    }

    @Test(dependsOnMethods = { "testCannyAvg", "testCannyAvg1" }, enabled = !DEBUG)
    public void testCannyAvgCombine() {
        combined1 = IOUtil.tryClone(cs1);
        combined1.combine(cs2);
        combined2 = IOUtil.tryClone(cs2);
        combined2.combine(cs1);

        complement1 = IOUtil.tryClone(cs);
        complement1.complement(cs1, false);
        complement2 = IOUtil.tryClone(cs);
        complement2.complement(cs2, false);

        Assert.assertTrue(Math.abs(cs.getValue() - combined1.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cs.getValue() - combined2.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cs2.getValue() - complement1.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cs1.getValue() - complement2.getValue()) < PREC);

    }

    // @Test(dependsOnMethods = { "testCannyRate", "testCannyRate1" }, enabled =
    // !DEBUG)
    // public void testCannyRateCombine() {
    // cr1.combine(cr2);
    // double err = Math.abs(cr.getValue() - cr1.getValue());
    // System.out.printf("combine error: %e\n", err);
    // Assert.assertTrue(err < PREC);
    // }

    @Test(dependsOnMethods = { "testCannyRate" }, enabled = !DEBUG)
    public void testCannyRateCombineAndSplit() {
        rcombined1 = IOUtil.tryClone(cr1);
        rcombined1.combine(cr2);
        rcombined2 = IOUtil.tryClone(cr2);
        rcombined2.combine(cr1);

        rcomplement1 = IOUtil.tryClone(cr);
        rcomplement1.complement(cr1, false);
        rcomplement2 = IOUtil.tryClone(cr);
        rcomplement2.complement(cr2, false);

        Assert.assertTrue(Math.abs(cr.getValue() - rcombined1.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cr.getValue() - rcombined2.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cr2.getValue() - rcomplement1.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cr1.getValue() - rcomplement2.getValue()) < PREC);

    }

    @Test(dependsOnMethods = { "testCannyRate1" }, enabled = !DEBUG)
    public void testCannyRateCombine() {
        rcombined1 = IOUtil.tryClone(cr1);
        rcombined1.combine(cr2);
        rcombined2 = IOUtil.tryClone(cr2);
        rcombined2.combine(cr1);

        Assert.assertTrue(Math.abs(cr.getValue() - rcombined1.getValue()) < PREC);
        Assert.assertTrue(Math.abs(cr.getValue() - rcombined2.getValue()) < PREC);
    }

    @DataProvider
    public Iterator<Object[]> dataprov() {
        // simulate 5-minute data
        t = System.currentTimeMillis();
        return new Iterator<Object[]>() {
            int n = N;

            @Override
            public boolean hasNext() {
                return n > 0;
            }

            @Override
            public Object[] next() {
                n--;
                CannySummarizerTest.this.n++;
                // in 10% of cases we may step back in history to simulate
                // backward history updates
                return new Object[] { t += rnd.nextDouble() < 0.1 ? -rnd.nextInt(2000) : rnd.nextInt(2000),
                    rnd.nextDouble() < 0.25 ? 1d : 0d };

            }

            @Override
            public void remove() {

            }
        };
    }

    @DataProvider
    public Iterator<Object[]> dataprov1() {
        // simulate 5-minute data
        t = System.currentTimeMillis();
        return new Iterator<Object[]>() {
            int n = N;

            @Override
            public boolean hasNext() {
                return n > 0;
            }

            @Override
            public Object[] next() {
                n--;
                CannySummarizerTest.this.n++;
                // in 10% of cases we may step back in history to simulate
                // backward history updates
                return new Object[] { t += (rnd.nextDouble() > 0.3 ? +1000 : -1000) + rnd.nextGaussian() * 33,
                    rnd.nextDouble() < 0.25 ? 1d : 0d };

            }

            @Override
            public void remove() {

            }
        };
    }

    @DataProvider
    public Iterator<Object[]> rateprov() {
        // simulate 1 per 1000 ms rate (i.e. 1e-3 per ms)
        t = System.currentTimeMillis();
        return new Iterator<Object[]>() {
            int n = N;

            @Override
            public boolean hasNext() {
                return n > 0;
            }

            @Override
            public Object[] next() {
                n--;
                CannySummarizerTest.this.n++;
                // in 10% of cases we may step back in history to simulate
                // backward history updates
                return new Object[] { t += Math.abs(1000 + rnd.nextGaussian()) * 33, 1 }; // +-10%
            }

            @Override
            public void remove() {

            }
        };
    }

    @DataProvider
    public Iterator<Object[]> rateprov1() {
        // simulate 1 per 1000 ms rate (i.e. 1e-3 per ms), irregular history

        t = System.currentTimeMillis();
        return new Iterator<Object[]>() {
            int n = N;

            @Override
            public boolean hasNext() {
                return n > 0;
            }

            @Override
            public Object[] next() {
                n--;
                CannySummarizerTest.this.n++;
                // in 10% of cases we may step back in history to simulate
                // backward history updates
                return new Object[] { t += (rnd.nextDouble() > 0.3 ? +1000 : -1000) + rnd.nextGaussian() * 33, 1 }; // +-10%
            }

            @Override
            public void remove() {

            }
        };
    }

}
