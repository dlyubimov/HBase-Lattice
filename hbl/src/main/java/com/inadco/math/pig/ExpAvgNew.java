package com.inadco.math.pig;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.inadco.math.aggregators.IrregularSamplingSummarizer;

/**
 * Creates are requested summarizer with given parameters and empty state.
 * 
 * @author dmitriy
 * 
 */
public class ExpAvgNew extends EvalFunc<DataByteArray> {

    private final DataByteArray m_result;

    public ExpAvgNew(String initStr) throws IOException {
        super();
        IrregularSamplingSummarizer sum = PigSummarizerHelper.createSummarizer(initStr, false);
        DataOutputBuffer dob = new DataOutputBuffer();
        PigSummarizerHelper.ser2bytes(sum, dob);
        m_result = new DataByteArray(dob.getData(), 0, dob.getLength());

    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        return m_result;
    }

    @Override
    public Type getReturnType() {
        return DataByteArray.class;
    }

}
