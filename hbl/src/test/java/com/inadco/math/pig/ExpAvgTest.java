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
package com.inadco.math.pig;

import java.util.Iterator;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
// import org.apache.pig.pigunit.PigTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inadco.math.aggregators.IrregularSamplingSummarizer;
import com.inadco.math.aggregators.OnlineCannyAvgSummarizer;

/**
 * This test needs $INADCO_HOME set.<P>
 * 
 * This actually requires PigUnit which is part of Pig only starting at Pig 0.9
 * 
 * @author dmitriy
 * 
 */
// @ContextConfiguration (
// {
// "classpath*:/common-components.xml",
// "classpath*:/hadoop-components.xml",
// "classpath:/com/inadco/math/pig/ExpAvgTest-context.xml"
// })


public class ExpAvgTest {

    final double[][]    input = { { 1, 22 }, { 2, 23 }, { 3, 24 } };
    final static double PREC  = 1E-10;

    /*
    @Test
    public void runExpAvgJob() throws Exception {

        IrregularSamplingSummarizer control = new OnlineCannyAvgSummarizer(6);

        String[] sinput = new String[input.length];
        for (int i = 0; i < sinput.length; i++) {
            sinput[i] = "A," + input[i][0] + "," + input[i][1];
            control.update(input[i][0], input[i][1]);
        }

        System.out.printf("control avg:%.4f\n", control.getValue());

        PigTest test = new PigTest("src/test/pig/expavgtest.pig");

        final String destination = "input.txt";
        PigTest.getCluster().copyFromLocalFile(sinput, destination, true);

        for (Iterator<Tuple> output = test.getAlias(); output.hasNext();) {
            Tuple t = output.next();
            double actual = DataType.toDouble(t.get(1));
            System.out.printf("control:%.4f, actual:%.4f\n", control.getValue(), actual);
            Assert.assertTrue(Math.abs(actual - control.getValue()) <= PREC);
        }

    }
    */
}
