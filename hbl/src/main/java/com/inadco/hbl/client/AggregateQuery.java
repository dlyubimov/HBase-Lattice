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
package com.inadco.hbl.client;

import java.io.IOException;

public interface AggregateQuery {

    AggregateQuery addMeasure(String measure);
    
    AggregateQuery addGroupBy(String dimName);

    AggregateQuery addClosedSlice(String dimension, Object leftBound, Object rightBound);

    AggregateQuery addOpenSlice(String dimension, Object leftBound, Object rightBound);

    AggregateQuery addHalfOpenSlice(String dimension, Object leftBound, Object rightBound);

    /**
     * 
     * @param dimension dimension to add
     * @param leftBound left bound value, null if unbounded
     * @param leftOpen
     * @param rightBound right bound value, null if unbounded 
     * @param rightOpen
     * @return
     */
    AggregateQuery
        addSlice(String dimension, Object leftBound, boolean leftOpen, Object rightBound, boolean rightOpen);

    AggregateResultSet execute() throws IOException;

    /**
     * reset for re-use
     */
    void reset(); 
}
