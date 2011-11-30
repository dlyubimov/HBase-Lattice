package com.inadco.math;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inadco.hbl.util.IOUtil;
import com.inadco.math.aggregators.OnlineCannyAvgSummarizer;
import com.inadco.math.aggregators.OnlineCannyRateSummarizer;

public class CannyTests2 {

    private final double PREC = 1E-10;

    @Test
    public void testCannyAvg1() {
        OnlineCannyAvgSummarizer sum = new OnlineCannyAvgSummarizer(5d), sum1 = new OnlineCannyAvgSummarizer(5d), sum2 =
            new OnlineCannyAvgSummarizer(5d);

        sum.update(1, 1);
        sum1.update(1, 1);

        Assert.assertTrue(Math.abs(sum.getValue() - 1d) < PREC);
        Assert.assertTrue(Math.abs(sum.getValueNow(5d) - 1d) < PREC);

        sum.update(4, 2);
        sum2.update(4, 2);

        Assert.assertTrue(Math.abs(sum.getValue() - sum.getValueNow(5d)) < PREC);

        OnlineCannyAvgSummarizer combined = IOUtil.tryClone(sum1);
        combined.combine(sum2);
        OnlineCannyAvgSummarizer compl1 = IOUtil.tryClone(sum);
        compl1.complement(sum1, false);
        OnlineCannyAvgSummarizer compl2 = IOUtil.tryClone(sum);
        compl2.complement(sum2, false);

        Assert.assertTrue(Math.abs(sum.getValue() - combined.getValue()) < PREC);
        Assert.assertTrue(Math.abs(sum1.getValue() - compl2.getValue()) < PREC);
        Assert.assertTrue(Math.abs(sum2.getValue() - compl1.getValue()) < PREC);
    }

    @Test(enabled = false)
    public void testCannyRate1() {
        OnlineCannyRateSummarizer sum = new OnlineCannyRateSummarizer(5d), sum1 = new OnlineCannyRateSummarizer(5d), sum2 =
            new OnlineCannyRateSummarizer(5d);

        sum.update(1, 1);
        sum1.update(1, 1);
        sum.update(2, 2);
        sum1.update(2, 2);

        sum.update(4, 4);
        sum2.update(4, 4);
//        sum1.update(0, 4); // connect sum1 to sum2 interval -- needed for
                           // complement to work.. well, at least one of the scenarios.

        sum.update(5, 5);
        sum2.update(5, 5);

        OnlineCannyRateSummarizer combined = IOUtil.tryClone(sum1);
        combined.combine(sum2);
        OnlineCannyRateSummarizer compl1 = IOUtil.tryClone(sum);
        compl1.complement(sum1, false);
        OnlineCannyRateSummarizer compl2 = IOUtil.tryClone(sum);
        compl2.complement(sum2, false);

        Assert.assertTrue(Math.abs(sum.getValue() - combined.getValue()) < PREC);
        Assert.assertTrue(Math.abs(sum2.getValue() - compl1.getValue()) < PREC);
        Assert.assertTrue(Math.abs(sum1.getValue() - compl2.getValue()) < PREC);
    }
}
