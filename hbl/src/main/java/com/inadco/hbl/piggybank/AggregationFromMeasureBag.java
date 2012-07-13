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
package com.inadco.hbl.piggybank;

import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.inadco.hbl.api.AggregateFunctionRegistry;
import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

/**
 * Takes bag of measure objects and returns {@link Aggregation} serialized into
 * {@link DataByteArray}.
 * 
 * Measures are members (if combine != 'y' ), otherwise, it is a serialized
 * protobuff {@link Aggregation} (if combine='y').
 * 
 * @author dmitriy
 * 
 */
public class AggregationFromMeasureBag extends EvalFunc<DataByteArray> implements Algebraic, Accumulator<DataByteArray> {

    private Initial delegate;

    public AggregationFromMeasureBag() {
        super();
    }

    public AggregationFromMeasureBag(String measureName, String bCombine, String encodedModel) {
        delegate = new Initial(measureName, bCombine, encodedModel);
    }

    /**
     * 
     * @param cube
     * @param measureName
     * @param afr
     * @param bag
     *            -- of original measures, usually Double or Integer per
     *            whatever is acceptable by particular measure's conversion by
     *            {@link Measure#compiler2Fact(Object)}.
     * @param accumulator
     * @return true if non-degenerate accumulation
     * @throws ExecException
     */
    private static boolean accumulateFromBag(Cube cube,
                                             String measureName,
                                             AggregateFunctionRegistry afr,
                                             Tuple bag,
                                             Aggregation.Builder accumulator) throws ExecException {
        Validate.notNull(bag, "bag argument is null");

        DataBag db = DataType.toBag(bag.get(0));
        Validate.notNull(db, "bag argument is null");

        Aggregation.Builder source;

        boolean nonDegenerate = false;

        for (Tuple tup : db) {
            Object d = compiler2Fact(cube, measureName, tup);
            if (d != null) {
                afr.applyAll(source = Aggregation.newBuilder(), d);
                afr.mergeAll(accumulator, source.build(), SliceOperation.ADD);
                nonDegenerate = true;
            }
        }
        return nonDegenerate;
    }

    /**
     * 
     * @param cube
     * @param measureName
     * @param afr
     * @param bag
     *            of {@link DataByteArray} members containing serialized
     *            {@link Aggregation} representation
     * @param accumulator
     * @preturn true if non-generate combining
     * @throws ExecException
     */
    private static boolean combineFromBag(Cube cube,
                                          String measureName,
                                          AggregateFunctionRegistry afr,
                                          Tuple bag,
                                          Aggregation.Builder accumulator) throws ExecException {
        Validate.notNull(bag, "bag argument is null");
        DataBag db = DataType.toBag(bag.get(0));
        Validate.notNull(db, "bag argument is null");
        boolean nonDegenerate = false;
        try {
            for (Tuple tup : db) {
                byte[] msg = DataType.toBytes(tup.get(0));
                if (msg == null)
                    continue; // should not happen.
                Aggregation source = Aggregation.parseFrom(msg);
                afr.mergeAll(accumulator, source, SliceOperation.ADD);
                nonDegenerate = true;
            }
            return nonDegenerate;
        } catch (InvalidProtocolBufferException exc) {
            throw new ExecException(exc);
        }

    }

    private static Object compiler2Fact(Cube cube, String measureName, Tuple input) throws ExecException {
        Measure m = cube.getMeasures().get(measureName);
        Validate.notNull(m, "no measure passed/found");

        Object val = input.get(0);
        Object d = m.compiler2Fact(val instanceof Tuple ? ((Tuple) val).getAll() : val);

        return d;
    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        Tuple t = delegate.exec(input);
        return t == null ? null : (DataByteArray) t.get(0);
    }

    @Override
    public void accumulate(Tuple b) throws IOException {
        delegate.accumulate(b);
    }

    @Override
    public DataByteArray getValue() {
        Tuple t = delegate.getValue();
        try {
            return t == null ? null : (DataByteArray) t.get(0);
        } catch (ExecException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public void cleanup() {
        delegate.cleanup();

    }

    @Override
    public String getInitial() {
        return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName();
    }

    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    public static class Initial extends BaseFunc<Tuple> implements Accumulator<Tuple> {

        protected String                    measureName;
        protected Aggregation.Builder       accumulator = Aggregation.newBuilder();
        protected boolean                   nonDegenerate;
        protected AggregateFunctionRegistry afr;
        protected boolean                   combine;

        public Initial() {
            super();
        }

        public Initial(String measureName, String combine, String encodedModel) {
            super(encodedModel);
            this.measureName = measureName;
            this.combine = (combine != null && "y".equals(combine));
            this.afr = super.cube.getAggregateFunctionRegistry();
        }

        @Override
        public void accumulate(Tuple b) throws IOException {

            if (!combine)
                nonDegenerate = accumulateFromBag(cube, measureName, afr, b, accumulator);
            else
                nonDegenerate = combineFromBag(cube, measureName, afr, b, accumulator);
        }

        @Override
        public Tuple getValue() {
            return nonDegenerate ? TupleFactory.getInstance().newTuple(new DataByteArray(accumulator.build()
                .toByteArray())) : null;
        }

        @Override
        public void cleanup() {
            accumulator = Aggregation.newBuilder();
        }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                accumulate(input);
                return getValue();
            } finally {
                cleanup();
            }
        }
    }

    public static class Intermediate extends Initial {

        @Override
        public void accumulate(Tuple b) throws IOException {
            nonDegenerate = combineFromBag(cube, measureName, afr, b, accumulator);
        }
    }

    public static class Final extends BaseFunc<DataByteArray> implements Accumulator<DataByteArray> {

        protected String                    measureName;
        protected Aggregation.Builder       accumulator = Aggregation.newBuilder();
        protected boolean                   nonDegenerate;
        protected AggregateFunctionRegistry afr;

        public Final() {
            super();
        }

        public Final(String measureName, String encodedModel) {
            super(encodedModel);
            this.measureName = measureName;
            this.afr = super.cube.getAggregateFunctionRegistry();
        }

        @Override
        public void accumulate(Tuple b) throws IOException {
            nonDegenerate = combineFromBag(cube, measureName, afr, b, accumulator);
        }

        @Override
        public void cleanup() {
            accumulator = Aggregation.newBuilder();
        }

        @Override
        public DataByteArray getValue() {
            return nonDegenerate ? new DataByteArray(accumulator.build().toByteArray()) : null;
        }

        @Override
        public DataByteArray exec(Tuple input) throws IOException {
            try {
                accumulate(input);
                return getValue();
            } finally {
                cleanup();
            }
        }

    }

}
