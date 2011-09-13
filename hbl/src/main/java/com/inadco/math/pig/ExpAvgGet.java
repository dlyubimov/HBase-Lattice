package com.inadco.math.pig;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.inadco.math.aggregators.IrregularSamplingSummarizer;

/**
 * Takes one parameter -- serializer state (bytearray) and optional second one
 * -- now time.
 * <P>
 * 
 * returns {@link IrregularSamplingSummarizer#getValue()} or (if second value is
 * supplied) {@link IrregularSamplingSummarizer#getValueNow(double)}.
 * <P>
 * 
 * Must be initialized with Pig's <code>define</code>, the sole parameter
 * accepts the initialization string for summarizer (see TDD for available
 * summarizer types and parameters).
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class ExpAvgGet extends EvalFunc<Double> {

    private final IrregularSamplingSummarizer m_summarizer;
    private final DataInputBuffer             m_dib = new DataInputBuffer();

    public ExpAvgGet(String initStr) {
        super();
        m_summarizer = PigSummarizerHelper.createSummarizer(initStr, true);
    }

    @Override
    public Double exec(Tuple input) throws IOException {
        byte[] bytes = DataType.toBytes(input.get(0));
        Double tNow = input.size() > 1 ? DataType.toDouble(input.get(1)) : null;

        if (bytes == null)
            return null;
        m_dib.reset(bytes, bytes.length);
        IrregularSamplingSummarizer sum = PigSummarizerHelper.bytes2ser(m_summarizer, m_dib);
        return sum == null ? null : (tNow == null ? sum.getValue() : sum.getValueNow(tNow));
    }

    @Override
    public Type getReturnType() {
        return Double.class;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            int inpSize = input.getFields().size();

            Validate.isTrue(inpSize == 1 || inpSize == 2, "Wrong argument number");
            Validate.isTrue(input.getField(0).type == DataType.BYTEARRAY, "wrong arg 1 type");
            Validate.isTrue(inpSize < 2 || DataType.isNumberType(input.getField(1).type), "wrong arg 2 type");

        } catch (FrontendException exc) {
            throw new IllegalArgumentException(exc);
        }

        return super.outputSchema(input);
    }

}
