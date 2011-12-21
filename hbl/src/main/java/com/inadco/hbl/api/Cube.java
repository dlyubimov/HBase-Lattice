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
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Cube  {
    
    String getName();

    Collection<? extends Cuboid> getCuboids();

    /**
     * finds cuboid with composite key order exactly as path
     * 
     * @param path
     * @return cuboid, or null if none exist
     */
    Cuboid findCuboidForPath(List<String> path);

    Cuboid findClosestSupercube(Set<String> dimensions);
    
    Map<String,? extends Measure> getMeasures();
    Map<String,? extends Dimension> getDimensions();
    
    AggregateFunctionRegistry getAggregateFunctionRegistry();

}
