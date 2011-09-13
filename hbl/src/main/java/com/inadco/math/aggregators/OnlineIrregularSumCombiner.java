package com.inadco.math.aggregators;

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
