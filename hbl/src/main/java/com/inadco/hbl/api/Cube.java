package com.inadco.hbl.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;

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

}
