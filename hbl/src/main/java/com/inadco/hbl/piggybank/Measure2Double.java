package com.inadco.hbl.piggybank;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.commons.lang.Validate;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.compiler.YamlModelParser;

public class Measure2Double extends BaseFunc<Double> {

    public Measure2Double(String encodedModel) {
        super(encodedModel);
        // measure = cube.getMeasures().get(measureName);
        // Validate.notNull(measure,
        // String.format("no measure %s found in the cube model", measureName));

    }

    @Override
    public Double exec(Tuple input) throws IOException {
        Object measureKey = input.get(0);
        Measure m = measureKey == null ? null : cube.getMeasures().get(measureKey);
        Validate.notNull(m, "no measure passed/found");
        Double d = m.asDouble(input.get(1));
        // we don't measures to evaluate to nulls to simplify null issues . 
        return d == null ? 0.0 : d;
    }

    @Override
    public Type getReturnType() {
        return Double.class;
    }

}
