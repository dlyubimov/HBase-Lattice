package com.inadco.hbl.math.pig;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.inadco.hbl.math.aggregators.IrregularSamplingSummarizer;

/**
 * This function accepts arguments -- bag of byte arrays -- representing
 * {@link IrregularSamplingSummarizer}'s serialized state and combines them to
 * produce one {@link IrregularSamplingSummarizer} representing unified history.
 * The summarizers are though to have been accumulated separate <b>disjoint</b>
 * subsets of the history.
 * <P>
 * 
 * 
 * @author dmitriy
 * 
 */

public class ExpAvgCombine extends EvalFunc<DataByteArray> {

    private final DataInputBuffer              m_dib = new DataInputBuffer();
    private final DataOutputBuffer             m_dob = new DataOutputBuffer();
    private final IrregularSamplingSummarizer  m_summarizer1;
    private final IrregularSamplingSummarizer  m_summarizer2;
    

    

    /**
     * Serializer's init string. however, it doesn't 
     * require anything but type here since this function 
     * technically never creates initialized instances, 
     * only buffers ("bare" instances). 
     * 
     * @param initStr
     */
    public ExpAvgCombine(String initStr ) {
        super();
        m_summarizer1=PigSummarizerHelper.createSummarizer(initStr, true);
        m_summarizer2=PigSummarizerHelper.createSummarizer(initStr, true);
        
    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        IrregularSamplingSummarizer result = null;
        for (Object obj : input.getAll()) {
            if ( obj == null ) continue; // one of arguments is null, ignore
            
            byte[] bytes = DataType.toBytes(obj);
            m_dib.reset(bytes, bytes.length);
            IrregularSamplingSummarizer sum =
                PigSummarizerHelper.bytes2ser(result == null ? m_summarizer1 : m_summarizer2, m_dib);
            if (result == null)
                result = sum;
            else
                result.combine(sum);
        }
        DataOutputBuffer dob = PigSummarizerHelper.ser2bytes(result, m_dob);
        return dob == null ? null : new DataByteArray(dob.getData(), 0, dob.getLength());
    }

    @Override
    public Schema outputSchema(Schema input) {
        for (FieldSchema fs : input.getFields())
            if (fs.type != DataType.BYTEARRAY)
                throw new IllegalArgumentException("Invalid input schema for ExpAvgCombine function.");

        return super.outputSchema(input);
    }

    @Override
    public Type getReturnType() {
        return DataByteArray.class;
    }

}
