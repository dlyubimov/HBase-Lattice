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
    private static final double       HISTORYLEN = 600000; // 10
    // MIN
    private static final int          N          = 600;
    private static final double       PREC       = 1E-5;

    private OnlineCannyAvgSummarizer  cs         = new OnlineCannyAvgSummarizer(HISTORYLEN),
        cs1 = new OnlineCannyAvgSummarizer(HISTORYLEN), cs2 = new OnlineCannyAvgSummarizer(HISTORYLEN);

    private OnlineCannyRateSummarizer cr         = new OnlineCannyRateSummarizer(HISTORYLEN),
        cr1 = new OnlineCannyRateSummarizer(HISTORYLEN), cr2 = new OnlineCannyRateSummarizer(HISTORYLEN);

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

        System.out.printf("cannyAvg: %.4f\n", cs.getValue());

    }

    @Test(dataProvider = "rateprov", enabled = !DEBUG)
    public void testCannyRate(double t, double x) {
        cr.update(x, t);

        if (rnd.nextDouble() > 0.5)
            cr1.update(x, t);
        else
            cr2.update(x, t);

        System.out.printf("cannyRate: %.4f\n", cr.getValue());

    }

    @Test(dataProvider = "rateprov1", enabled = !DEBUG)
    public void testCannyRate1(double t, double x) {
        testCannyRate(t, x);
        System.out.printf("cannyRate1: %.4f\n", cr.getValue());
    }

    @Test(dependsOnMethods = { "testCannyAvg", }, enabled = !DEBUG)
    public void testCannyAvgCombine() {
        cs1.combine(cs2);
        double err = Math.abs(cs.getValue() - cs1.getValue());
        System.out.printf("combine error: %e\n", err);
        Assert.assertTrue(err < PREC);
    }

    @Test(dependsOnMethods = { "testCannyRate", "testCannyRate1" }, enabled = !DEBUG)
    public void testCannyRateCombine() {
        cr1.combine(cr2);
        double err = Math.abs(cr.getValue() - cr1.getValue());
        System.out.printf("combine error: %e\n", err);
        Assert.assertTrue(err < PREC);
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
                return new Object[] { t += 1000 + rnd.nextGaussian() * 33, 1 }; // +-10%
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
