package com.inadco.hbl.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Measure;

/**
 * 
 * @author dmitriy
 * 
 */
public class SimpleCube implements Cube {

    protected String                    name;
    protected Map<String, Dimension>    dimensions       = new HashMap<String, Dimension>();
    protected Map<String, Dimension>    readonlyDims     = Collections.unmodifiableMap(dimensions);

    // mapped by "cuboid path" which is
    // the combination of dimensions in the composite hbase key
    // specified by name.
    protected Map<List<String>, Cuboid> cuboids          = new HashMap<List<String>, Cuboid>();

    protected Map<String, Measure>      measures         = new HashMap<String, Measure>();
    protected Map<String, Measure>      readonlyMeasures = Collections.unmodifiableMap(measures);

    /**
     * constructor
     * 
     * @param name
     * @param dimensions
     * @param cuboids
     * @param measures
     */
    public SimpleCube(String name, Dimension[] dimensions, Cuboid[] cuboids, Measure[] measures) {
        super();
        this.name = name;
        for (Dimension dim : dimensions)
            this.dimensions.put(dim.getName(), dim);
        for (Cuboid c : cuboids) {
            this.cuboids.put(c.getCuboidPath(), c);
            c.setTablePrefix(name + "_");
        }
        for (Measure m : measures)
            this.measures.put(m.getName(), m);
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

}
