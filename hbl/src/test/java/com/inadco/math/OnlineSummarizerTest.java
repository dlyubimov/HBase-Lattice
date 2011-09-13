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

import com.inadco.math.aggregators.OnlineExpBiasedBinomialSummarizer;
import com.inadco.math.aggregators.OnlineExpRateSummarizer;
import com.inadco.math.aggregators.OnlineMeanSummarizer;

public class OnlineSummarizerTest {

    private static final boolean                               DEBUG      = false;

    private static final double                                BIAS       = 0.75;
    private static final double                                CONVERGED  = 0.25;
    private static final double                                HISTORYLEN = 600000; // 10
    // MIN
    private static final int                                   N          = 600;
    private static final double                                PREC       = 1E-7;

    private OnlineExpBiasedBinomialSummarizer s          = new OnlineExpBiasedBinomialSummarizer(
                                                                              BIAS,
                                                                              HISTORYLEN), // phased
                                                                                           // out
                                                                                           // at
                                                                                           // 1%
                                                                                           // after
                                                                                           // 10
                                                                                           // mins
        split1 = new OnlineExpBiasedBinomialSummarizer(BIAS, HISTORYLEN),
        split2 = new OnlineExpBiasedBinomialSummarizer(BIAS, HISTORYLEN);

    private OnlineExpRateSummarizer                    rs         = new OnlineExpRateSummarizer(
                                                                              HISTORYLEN),
        rs1 = new OnlineExpRateSummarizer(HISTORYLEN), rs2 = new OnlineExpRateSummarizer(HISTORYLEN);

    private double                                             t;
    private Random                                             rnd        = new Random();
    private int                                                n;

    @Test(dataProvider = "dataprov", enabled = !DEBUG)
    void testBinomialExpWeightedWithIrregSam(double t, double x) throws Exception {
        double p = s.update(x, t);

        System.out.printf("ExpAvg: %.4f\n", p);

        if (n > 10)
            assert Math.abs(p - CONVERGED) <= 0.3; // at least no worse than
                                                   // regular beta

        if (rnd.nextDouble() > 0.5)
            split1.update(x, t);
        else
            split2.update(x, t);

    }

    @Test(dependsOnMethods = "testBinomialExpWeightedWithIrregSam", enabled = !DEBUG)
    void combineSplits() {
        split1.combine(split2);
    }

    @Test(dependsOnMethods = "combineSplits", invocationCount = 20, enabled = !DEBUG)
    void testAssertion() throws Exception {
        double time = t + rnd.nextInt(20000);

        double p = s.pnow(time);
        double pComb = split1.pnow(time);

        Assert.assertTrue(Math.abs(p - pComb) <= PREC);

        // assert Math.abs(p-CONVERGED)<=1/(2+N); // at least no worse than
        // regular beta
        System.out.printf("Binomial distribution prob after N samples: %.4f\n", p);
    }

    @Test(enabled = !DEBUG)
    public void testRateUpdateInThepPast() {
        rs.reset();
        rs1.reset();

        double t = System.currentTimeMillis();

        // we update rs and rs1 in different order and assert they are the same.

        rs.update(1, t);
        rs.update(1, t + 1000);

        rs1.update(1, t + 1000);
        rs1.update(1, t);

        double err = Math.abs(rs.getRate() - rs1.getRate());
        System.out.printf("error for update-in-the past: %e\n", err);

        Assert.assertTrue(err < PREC);

    }

    @Test(dataProvider = "rateprov", enabled = !DEBUG)
    public void testRate(double t, double x) {
        double r = rs.update(x, t);

        System.out.printf("ratesum: %.4e\n", r);

        if (rnd.nextDouble() > 0.5)
            rs1.update(x, t);
        else
            rs2.update(x, t);

    }

    @Test(dataProvider = "rateprov1", enabled = !DEBUG)
    public void testRateInThePast(double t, double x) {
        double r = rs.update(x, t);

        System.out.printf("ratesum-in-the-past: %.4e\n", r);

        if (rnd.nextDouble() > 0.5)
            rs1.update(x, t);
        else
            rs2.update(x, t);

    }

    @Test(dependsOnMethods = { "testRate", "testRateInThePast" }, enabled = !DEBUG)
    public void testRateCombine() {
        rs1.combine(rs2);
        double err = Math.abs(rs.getRate() - rs1.getRate());
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
                OnlineSummarizerTest.this.n++;
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
                OnlineSummarizerTest.this.n++;
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
                OnlineSummarizerTest.this.n++;
                // in 10% of cases we may step back in history to simulate
                // backward history updates
                return new Object[] { t += (rnd.nextDouble() > 0.3 ? +1000 : -1000) + rnd.nextGaussian() * 33, 1 }; // +-10%
            }

            @Override
            public void remove() {

            }
        };
    }

    @Test(enabled = !DEBUG)
    public void OnlineMeanSummarizerTest() throws Exception {
        Random rnd = new Random();
        OnlineMeanSummarizer oms = new OnlineMeanSummarizer();

        for (int i = 0; i < 10000; i++)
            oms.update(rnd.nextGaussian() * 5 + 1); // should generate mean 1,
                                                 // variance 25 or so
        Assert.assertTrue(Math.abs(oms.getMean() - 1) < 0.1);
        Assert.assertTrue(Math.sqrt(oms.getVariance()) - 25 < 5);

    }

}
