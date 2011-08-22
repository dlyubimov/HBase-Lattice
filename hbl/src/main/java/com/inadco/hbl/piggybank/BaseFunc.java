package com.inadco.hbl.piggybank;

import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.pig.EvalFunc;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.compiler.YamlModelParser;

public abstract class BaseFunc<T> extends EvalFunc<T> {

    protected Cube cube;
    
    public BaseFunc( String encodedModel ) {
        super();
        try {
            cube = YamlModelParser.decodeCubeModel(encodedModel);
            Validate.notNull(cube, "no cube model found in the job conf");
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
    

}
