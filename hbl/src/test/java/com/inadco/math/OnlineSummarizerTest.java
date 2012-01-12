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

import com.inadco.hbl.math.aggregators.OnlineExpBiasedBinomialSummarizer;
import com.inadco.hbl.math.aggregators.OnlineExpRateSummarizer;
import com.inadco.hbl.math.aggregators.OnlineMeanSummarizer;
import com.inadco.hbl.util.IOUtil;

public class OnlineSummarizerTest {

    private static final boolean              DEBUG      = false;

    private static final double               BIAS       = 0.75;
    private static final double               CONVERGED  = 0.25;
    private static final double               HISTORYLEN = 600000;      // 10
    // MIN
    private static final int                  N          = 200;
    private static final double               PREC       = 1E-7;

    private OnlineExpBiasedBinomialSummarizer s          = new OnlineExpBiasedBinomialSummarizer(BIAS, HISTORYLEN), // phased
                                                                                                                    // out
                                                                                                                    // at
                                                                                                                    // 1%
                                                                                                                    // after
                                                                                                                    // 10
                                                                                                                    // mins
        split1 = new OnlineExpBiasedBinomialSummarizer(BIAS, HISTORYLEN),
        split2 = new OnlineExpBiasedBinomialSummarizer(BIAS, HISTORYLEN),
        combined1,
        combined2,
        complement1,
        complement2;

    private OnlineExpRateSummarizer           rs         = new OnlineExpRateSummarizer(HISTORYLEN),
        rs1 = new OnlineExpRateSummarizer(HISTORYLEN), rs2 = new OnlineExpRateSummarizer(HISTORYLEN), rcombined1,
        rcombined2, rcomplement1, rcomplement2;

    private double                            t;
    private Random                            rnd        = new Random();
    private int                               n;

    @Test(dataProvider = "dataprov", enabled = !DEBUG)
    void testBinomialExpWeightedWithIrregSam(double t, double x) throws Exception {
        double p = s.update(x, t);

        System.out.printf("ExpAvg: %.4f\n", p);

        if (n > 20)
            assert Math.abs(p - CONVERGED) <= 0.3; // at least no worse than
                                                   // regular beta

        if (rnd.nextDouble() > 0.5)
            split1.update(x, t);
        else
            split2.update(x, t);

    }

    @Test(dependsOnMethods = "testBinomialExpWeightedWithIrregSam", enabled = !DEBUG)
    void combineSplits() {
        combined1 = IOUtil.tryClone(split1);
        combined1.combine(split2);
        combined2 = IOUtil.tryClone(split2);
        combined2.combine(split1);

        complement1 = IOUtil.tryClone(s);
        complement1.complement(split1, false);
        complement2 = IOUtil.tryClone(s);
        complement2.complement(split2, false);

    }

    @Test(dependsOnMethods = "combineSplits",  enabled = !DEBUG)
    void testAssertion() throws Exception {
        double time = t + rnd.nextInt(20000);

        double p = s.pnow(time);
        double pComb1 = combined1.pnow(time);
        double pComb2 = combined2.pnow(time);

        Assert.assertTrue(Math.abs(p - pComb1) <= PREC);
        Assert.assertTrue(Math.abs(p - pComb2) <= PREC);

        System.out.printf("Binomial distribution prob after N samples: %.4f\n", p);

        // for complements, any biased estimation will be off.
        // we'll be asserting that complement of split 1 == split 2
        // and complement of split 2 == split 1
        double valComp1 = complement1.getValue();
        double valComp2 = complement2.getValue();

        Assert.assertTrue(Math.abs(valComp1 - split2.getValue()) <= PREC);
        Assert.assertTrue(Math.abs(valComp2 - split1.getValue()) <= PREC);
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

    private OnlineExpRateSummarizer currSplit = null;

    @Test(dataProvider = "rateprov", enabled = true)
    public void testRate(double t, double x) {
        double r = rs.update(x, t);

        System.out.printf("ratesum: %.4e\n", r);

        // in order to test complementing the rates,
        // we need to ensure splits are continuous.
        // complement doesn't work with anything else.

        if (currSplit == null)
            currSplit = rs1;
        else if (currSplit == rs1 && rnd.nextDouble() < 0.01) {
            currSplit = rs2;
            // in order for test to work, we have to 
            // "pull" amount of time in rs1 so that it connects 
            // to the start point of rs2, so 
            rs1.update(0, t);
            // of course it is not going to be the case in the
            // in projections, unless we explicitly update the rate 
            // summarizers there with hierarchy boundaries!
        }

        currSplit.update(x, t);

    }

    @Test(dataProvider = "rateprov1", dependsOnMethods = "testRateCombine", enabled = !DEBUG)
    public void testRateInThePast(double t, double x) {
        double r = rs.update(x, t);

        System.out.printf("ratesum-in-the-past: %.4e\n", r);

        if (rnd.nextDouble() > 0.5)
            rs1.update(x, t);
        else
            rs2.update(x, t);

    }

    @Test(dependsOnMethods = { "testRate" }, enabled = !DEBUG)
    public void testRateCombine() {
        rcombined1 = IOUtil.tryClone(rs1);
        rcombined1.combine(rs2);
        rcombined2 = IOUtil.tryClone(rs2);
        rcombined2.combine(rs1);

        rcomplement1 = IOUtil.tryClone(rs);
        rcomplement1.complement(rs1, false);
        rcomplement2 = IOUtil.tryClone(rs);
        rcomplement2.complement(rs2, false);


        Assert.assertTrue(Math.abs(rs.getRate() - rcombined1.getRate()) < PREC);
        Assert.assertTrue(Math.abs(rs.getRate() - rcombined2.getRate()) < PREC);
        Assert.assertTrue(Math.abs(rcomplement1.getRate() - rs2.getRate()) < PREC);
        Assert.assertTrue(Math.abs(rcomplement2.getRate() - rs1.getRate()) < PREC);

    }

    @Test(dependsOnMethods = { "testRateInThePast" }, enabled = !DEBUG)
    public void testRateCombineInThePast() {
        rcombined1 = IOUtil.tryClone(rs1);
        rcombined1.combine(rs2);
        rcombined2 = IOUtil.tryClone(rs2);
        rcombined2.combine(rs1);

        double err = Math.abs(rs.getRate() - rs1.getRate());
        System.out.printf("combine error: %e\n", err);

        Assert.assertTrue(Math.abs(rs.getRate() - rcombined1.getRate()) < PREC);
        Assert.assertTrue(Math.abs(rs.getRate() - rcombined2.getRate()) < PREC);

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
                return new Object[] { t += Math.abs(1000 + rnd.nextGaussian() * 33), 1 }; // +-10%
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
