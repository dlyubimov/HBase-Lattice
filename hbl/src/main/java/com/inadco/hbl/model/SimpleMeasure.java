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
package com.inadco.hbl.model;

import com.inadco.hbl.api.Measure;

/**
 * Simple double-measure support 
 * 
 * @author dmitriy
 *
 */
public class SimpleMeasure implements Measure {

    protected String name;
    
    public SimpleMeasure(String name) {
        super();
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object compiler2Fact(Object value) {
        if (value == null)
            return null;
        /*
         * translate all numeric types to Double.
         */
        else if (value instanceof Double)
            return (Double) value;
        else if (value instanceof Long)
            return ((Long) value).doubleValue();
        else if (value instanceof Integer)
            return ((Integer) value).doubleValue();
        else if (value instanceof Short)
            return ((Short) value).doubleValue();
        else if (value instanceof Byte)
            return ((Byte) value).doubleValue();
        else
            throw new RuntimeException(String.format("Unknown measure instance type: %s", value.getClass().getName()));
    }


}
