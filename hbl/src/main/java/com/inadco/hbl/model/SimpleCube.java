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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.AggregateFunction;
import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Measure;

/**
 * Simple cube model implementation.
 * 
 * @author dmitriy
 * 
 */
public class SimpleCube implements Cube {

    protected String                          name;
    protected Map<String, Dimension>          dimensions       = new HashMap<String, Dimension>();
    protected Map<String, Dimension>          readonlyDims     = Collections.unmodifiableMap(dimensions);

    // mapped by "cuboid path" which is
    // the combination of dimensions in the composite hbase key
    // specified by name.
    protected Map<List<String>, Cuboid>       cuboids          = new HashMap<List<String>, Cuboid>();

    protected Map<String, Measure>            measures         = new HashMap<String, Measure>();
    protected Map<String, Measure>            readonlyMeasures = Collections.unmodifiableMap(measures);
    protected SimpleAggregateFunctionRegistry afr;
    protected long                            ms;

    /**
     * constructor
     * 
     * @param name
     *            cube name
     * @param dimensions
     *            list (set) of dimensions
     * @param cuboids
     *            sequence of cuboids. Order important.
     * @param measures
     *            list (set) of measures
     */
    public SimpleCube(String name, Dimension[] dimensions, Cuboid[] cuboids, Measure[] measures) {
        super();
        this.name = name;
        for (Dimension dim : dimensions)
            this.dimensions.put(dim.getName(), dim);
        for (Cuboid c : cuboids) {
            this.cuboids.put(c.getCuboidPath(), c);
            c.setTablePrefix(name + "_");
            if (c instanceof SimpleCuboid)
                ((SimpleCuboid) c).setParentCube(this);
        }
        for (Measure m : measures)
            this.measures.put(m.getName(), m);
        this.afr = new SimpleAggregateFunctionRegistry();
        ms = System.currentTimeMillis();
    }

    public SimpleCube(String name,
                      Dimension[] dimensions,
                      Cuboid[] cuboids,
                      Measure[] measures,
                      AggregateFunction[] customFunctions) {
        this(name, dimensions, cuboids, measures);
        for (AggregateFunction cf : customFunctions)
            afr.addFunction(cf);
    }

    public String getName() {
        return name;
    }

    @Override
    public Collection<? extends Cuboid> getCuboids() {
        return Collections.unmodifiableCollection(cuboids.values());
    }

    @Override
    public Cuboid findCuboidForPath(List<String> path) {
        Validate.notNull(path);
        return cuboids.get(path);
    }

    @Override
    public Cuboid findClosestSupercube(Set<String> dimensions) {
        // reserved
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ? extends Measure> getMeasures() {
        return readonlyMeasures;
    }

    @Override
    public Map<String, ? extends Dimension> getDimensions() {
        return readonlyDims;
    }

    @Override
    public SimpleAggregateFunctionRegistry getAggregateFunctionRegistry() {
        return afr;
    }

    public long getTimestamp() {
        return ms;
    }

}
