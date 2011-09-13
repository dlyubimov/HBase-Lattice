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

import com.google.protobuf.InvalidProtocolBufferException;
import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.scanner.AggregateFunctionRegistry;
import com.inadco.hbl.scanner.SliceOperation;

/**
 * Takes bag of measure objects and returns {@link Aggregation} serialized into
 * {@link DataByteArray}.
 * 
 * @author dmitriy
 * 
 */
public class AggregationFromMeasureBag extends EvalFunc<DataByteArray> implements Algebraic, Accumulator<DataByteArray> {

    private Initial delegate;

    public AggregationFromMeasureBag(String measureName, String encodedModel) {
        delegate = new Initial(measureName, encodedModel);
    }

    /**
     * 
     * @param cube
     * @param measureName
     * @param afr
     * @param bag
     *            -- of original measures, usually Double or Integer per
     *            whatever is acceptable by particular measure's conversion by
     *            {@link Measure#asDouble(Object)}.
     * @param accumulator
     * @throws ExecException
     */
    private static void accumulateFromBag(Cube cube,
                                          String measureName,
                                          AggregateFunctionRegistry afr,
                                          Tuple bag,
                                          Aggregation.Builder accumulator) throws ExecException {
        Validate.notNull(bag, "bag argument is null");

        DataBag db = DataType.toBag(bag.get(0));
        Validate.notNull(db, "bag argument is null");

        Aggregation.Builder source;

        for (Tuple tup : db) {
            Double d = measure2Double(cube, measureName, tup);
            afr.applyAll(source = Aggregation.newBuilder(), d);
            afr.mergeAll(accumulator, source.build(), SliceOperation.ADD);
        }
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
     * @throws ExecException
     */
    private static void combineFromBag(Cube cube,
                                       String measureName,
                                       AggregateFunctionRegistry afr,
                                       Tuple bag,
                                       Aggregation.Builder accumulator) throws ExecException {
        Validate.notNull(bag, "bag argument is null");
        DataBag db = DataType.toBag(bag.get(0));
        Validate.notNull(db, "bag argument is null");
        try {
            for (Tuple tup : db) {
                byte[] msg = DataType.toBytes(tup.get(0));
                if (msg == null)
                    continue; // should not happen.
                Aggregation source = Aggregation.parseFrom(msg);
                afr.mergeAll(accumulator, source, SliceOperation.ADD);
            }
        } catch (InvalidProtocolBufferException exc) {
            throw new ExecException(exc);
        }

    }

    private static Double measure2Double(Cube cube, String measureName, Tuple input) throws ExecException {
        Measure m = cube.getMeasures().get(measureName);
        Validate.notNull(m, "no measure passed/found");
        Double d = m.asDouble(input.get(0));
        // we don't measures to evaluate to nulls to simplify null issues.
        return d == null ? 0.0 : d;
    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        return delegate.exec(input);
    }

    @Override
    public void accumulate(Tuple b) throws IOException {
        delegate.accumulate(b);
    }

    @Override
    public DataByteArray getValue() {
        return delegate.getValue();
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
        return Final.class.getName();
    }

    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    public class Initial extends BaseFunc<DataByteArray> implements Accumulator<DataByteArray> {

        protected String                    measureName;
        protected Aggregation.Builder       accumulator = Aggregation.newBuilder();
        protected AggregateFunctionRegistry afr         = new AggregateFunctionRegistry();

        public Initial() {
            super();
        }

        public Initial(String measureName, String encodedModel) {
            super(encodedModel);
            this.measureName = measureName;
        }

        @Override
        public void accumulate(Tuple b) throws IOException {
            accumulateFromBag(cube, measureName, afr, b, accumulator);
        }

        @Override
        public DataByteArray getValue() {
            return new DataByteArray(accumulator.build().toByteArray());
        }

        @Override
        public void cleanup() {
            accumulator = Aggregation.newBuilder();
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

    public class Final extends Initial {

        public Final() {
            super();
        }

        public Final(String measureName, String encodedModel) {
            super(measureName, encodedModel);
        }

        @Override
        public void accumulate(Tuple b) throws IOException {
            combineFromBag(cube, measureName, afr, b, accumulator);
        }

    }

}
