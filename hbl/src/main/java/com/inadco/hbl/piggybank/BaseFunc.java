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
import org.apache.pig.EvalFunc;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.compiler.YamlModelParser;
/**
 * Common udf functionality
 * @author dmitriy
 *
 * @param <T>
 */
public abstract class BaseFunc<T> extends EvalFunc<T> {

    protected Cube cube;
    
    protected BaseFunc() { 
        
    }
    
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
