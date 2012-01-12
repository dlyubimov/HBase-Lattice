package com.inadco.hbl.math.pig;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.inadco.hbl.math.aggregators.IrregularSamplingSummarizer;

/**
 * Accepts parameters:
 * <P>
 * 
 * bag of (x,t) observations
 * 
 * size of 2 should be the same as size of 3
 * 
 * @author dmitriy
 * 
 */

public class ExpAvgUpdate extends EvalFunc<DataByteArray> implements Algebraic, Accumulator<DataByteArray> {

    private EAInitial m_delegate;

    public ExpAvgUpdate(String dtStr) {
        super();
        m_delegate = new EAInitial(dtStr);
    }

    public void accumulate(Tuple bag) throws IOException {
        m_delegate.accumulate(bag);
    }

    public DataByteArray exec(Tuple input) throws IOException {
        return (DataByteArray) m_delegate.exec(input).get(0);
    }

    public DataByteArray getValue() {
        try {
            return (DataByteArray) m_delegate.getValue().get(0);
        } catch (IOException exc) {
            throw new IllegalArgumentException(exc);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public String getInitial() {
        return EAInitial.class.getName();
    }

    @Override
    public String getIntermed() {
        return EAIntermediate.class.getName();
    }

    @Override
    public String getFinal() {
        return EAFinal.class.getName();
    }

    @Override
    public Type getReturnType() {
        return DataByteArray.class;
    }

    @Override
    public Schema outputSchema(Schema input) {

        try {

            // this is strange. some of invocations send in {sample:(x,t)} and
            // some send {{sample:(x,t)}} for obviously
            // same invocation pattern.

            if (input.size() != 1 || DataType.BAG != input.getField(0).type)
                throw new IllegalArgumentException("Bag argument expected");
            Schema bagTupleSchema = input.getField(0).schema;

            // workaround for what seems to be a pig bug?
            if (bagTupleSchema.getField(0).type == DataType.TUPLE)
                bagTupleSchema = bagTupleSchema.getField(0).schema;

            List<FieldSchema> fields = bagTupleSchema.getFields();
            if (fields.size() != 2 || !DataType.isNumberType(fields.get(0).type)
                || !DataType.isNumberType(fields.get(1).type))

                throw new IllegalArgumentException("Bag of 2 doubles expected");

            return super.outputSchema(input);
        } catch (FrontendException exc) {
            throw new RuntimeException(exc);
        }
    }

    static IrregularSamplingSummarizer accumulate(Tuple bag, IrregularSamplingSummarizer summarizer, String initStr)
        throws IOException {
        if (summarizer == null)
            summarizer = PigSummarizerHelper.createSummarizer(initStr, false);

        DataBag db = DataType.toBag(bag.get(0));
        if (db == null)
            throw new IOException("bag argument is null");

        for (Tuple tup : db) {
            Double x = DataType.toDouble(tup.get(0));
            Double t = DataType.toDouble(tup.get(1));
            if (x == null || tup == null)
                throw new IOException("bad x or  t parameter");
            summarizer.update(x, t);
        }
        return summarizer;
    }

    public static class EAInitial extends EvalFunc<Tuple> implements Accumulator<Tuple> {

        private String                      m_initStr;
        private IrregularSamplingSummarizer m_summarizer;
        private DataOutputBuffer            m_dob = new DataOutputBuffer();

        public EAInitial() {
            super();
        }

        public EAInitial(String initStr) {
            super();
            m_initStr = initStr;
        }

        @Override
        public void accumulate(Tuple bag) throws IOException {
            m_summarizer = ExpAvgUpdate.accumulate(bag, m_summarizer, m_initStr);
        }

        @Override
        public Tuple getValue() {
            try {
                if (m_summarizer == null)
                    // group's '0'
                    m_summarizer = PigSummarizerHelper.createSummarizer(m_initStr, false);

                DataOutputBuffer dob = PigSummarizerHelper.ser2bytes(m_summarizer, m_dob);
                return TupleFactory.getInstance().newTuple(dob == null ? null : new DataByteArray(
                    dob.getData(),
                    0,
                    dob.getLength()));
            } catch (IOException exc) {
                throw new RuntimeException(exc);
            }
        }

        @Override
        public void cleanup() {
            m_summarizer = null;

        }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                m_summarizer = null;
                accumulate(input);
                return getValue();
            } finally {
                m_summarizer = null;
            }

        }
    }

    public static class EAIntermediate extends EvalFunc<Tuple> implements Accumulator<Tuple> {

        private String                      m_initStr;
        private IrregularSamplingSummarizer m_summarizer;
        private IrregularSamplingSummarizer m_buffer;
        private DataOutputBuffer            m_dob = new DataOutputBuffer();
        private DataInputBuffer             m_dib = new DataInputBuffer();

        public EAIntermediate() {
            super();
        }

        public EAIntermediate(String initStr) {
            super();
            m_initStr = initStr;
            m_buffer = PigSummarizerHelper.createSummarizer(initStr, true);
        }

        @Override
        public void accumulate(Tuple b) throws IOException {
            m_summarizer = PigSummarizerHelper.combine(b, m_dib, m_initStr, m_summarizer, m_buffer);
        }

        @Override
        public Tuple getValue() {
            try {
                DataOutputBuffer dob = PigSummarizerHelper.ser2bytes(m_summarizer, m_dob);
                return TupleFactory.getInstance().newTuple(dob == null ? null : new DataByteArray(
                    dob.getData(),
                    0,
                    dob.getLength()));
            } catch (IOException exc) {
                throw new RuntimeException(exc);
            }
        }

        @Override
        public void cleanup() {
            m_summarizer = null;
        }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                m_summarizer = null;
                accumulate(input);
                return getValue();
            } finally {
                m_summarizer = null;
            }
        }

    }

    public static class EAFinal extends EvalFunc<DataByteArray> implements Accumulator<DataByteArray> {

        private String                      m_initStr;
        private IrregularSamplingSummarizer m_summarizer;
        private IrregularSamplingSummarizer m_buffer;
        private DataOutputBuffer            m_dob = new DataOutputBuffer();
        private DataInputBuffer             m_dib = new DataInputBuffer();

        public EAFinal() {
            super();
        }

        public EAFinal(String initStr) {
            super();
            m_buffer = PigSummarizerHelper.createSummarizer(initStr, true);
            m_initStr = initStr;
        }

        @Override
        public void accumulate(Tuple b) throws IOException {
            m_summarizer = PigSummarizerHelper.combine(b, m_dib, m_initStr, m_summarizer, m_buffer);
        }

        @Override
        public DataByteArray getValue() {
            try {
                DataOutputBuffer dob = PigSummarizerHelper.ser2bytes(m_summarizer, m_dob);
                return dob == null ? null : new DataByteArray(dob.getData(), 0, dob.getLength());
            } catch (IOException exc) {
                throw new RuntimeException(exc);
            }
        }

        @Override
        public void cleanup() {
            m_summarizer = null;
        }

        @Override
        public DataByteArray exec(Tuple input) throws IOException {
            try {
                m_summarizer = null;
                accumulate(input);
                return getValue();
            } finally {
                m_summarizer = null;
            }
        }

    }

}
