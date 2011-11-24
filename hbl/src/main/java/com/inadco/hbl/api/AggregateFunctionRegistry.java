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
package com.inadco.hbl.api;

import java.util.Collection;

import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

/**
 * Aggregate function registry supporting certain compilation and query
 * operations over aggregate function space.
 * 
 * @author dmitriy
 * 
 */
public interface AggregateFunctionRegistry {

    void applyAll(Aggregation.Builder accumulator, Object measure);

    void mergeFunctions(Collection<String> funcNames,
                        Aggregation.Builder accumulator,
                        Aggregation source,
                        SliceOperation operation);

    void mergeAll(Aggregation.Builder accumulator, Aggregation source, SliceOperation operation);

    AggregateFunction findFunction(String name);
}
