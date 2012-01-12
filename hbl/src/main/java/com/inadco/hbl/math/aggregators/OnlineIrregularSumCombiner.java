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
package com.inadco.hbl.math.aggregators;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Default combiner and perhaps sometimes reducer too for combining exp weighted
 * summarizer states
 * 
 * @author dmitriy
 * 
 */

public class OnlineIrregularSumCombiner extends
    Reducer<Writable, IrregularSamplingSummarizer, Writable, IrregularSamplingSummarizer> {

    private IrregularSamplingSummarizer m_buffer = null;

    @Override
    protected void reduce(Writable key, Iterable<IrregularSamplingSummarizer> values, Context ctx) throws IOException,
        InterruptedException {

        IrregularSamplingSummarizer result = null;
        for (IrregularSamplingSummarizer val : values) {
            if (result == null) {
                result = m_buffer = ReflectionUtils.newInstance(val.getClass(), ctx.getConfiguration());
                m_buffer.assign(val);
            } else {
                result.combine(val);
            }
        }
        if (result != null)
            ctx.write(key, result);
    }
}
