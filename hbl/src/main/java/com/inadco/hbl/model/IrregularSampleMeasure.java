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

import java.util.List;

import org.apache.commons.lang.Validate;

/**
 * Measure that accets tuples in form of ( x, t ) where x is double and t is
 * 64bit long containing ms since epoch
 * 
 * @author dmitriy
 * 
 */
public class IrregularSampleMeasure extends SimpleMeasure {

    public IrregularSampleMeasure(String name) {
        super(name);
    }

    @Override
    public Object compiler2Fact(Object value) {
        if (value == null)
            return null;
        if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> l = (List<Object>) value;

            Validate.isTrue(l.size() == 2, "irregular sample facts must be tuple(x,t)");

            Object x = super.compiler2Fact(l.get(0));
            Object tobj = l.get(1);
            Long t = null;

            if (x == null || tobj == null)
                return null;

            if (tobj instanceof Long)
                t = (Long) tobj;
            else if (tobj instanceof Integer)
                t = ((Integer) tobj).longValue();
            else if (tobj instanceof Short)
                t = ((Short) tobj).longValue();
            else
                throw new RuntimeException(String.format("Unsupported measure sample time type: %s", tobj.getClass()
                    .getName()));
            return new IrregularSample(x, t);
        } else
            throw new RuntimeException(String.format("Unknown/unsupported measure instance type: %s", value.getClass()
                .getName()));
    }

}
