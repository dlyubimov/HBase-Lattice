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

import java.io.IOException;
import java.util.List;

import com.inadco.hbl.compiler.Pig8CubeIncrementalCompilerBean;

/**
 * Cuboid api
 * 
 * @author dmitriy
 * 
 */
public interface Cuboid {

    List<String> getCuboidPath();

    List<Dimension> getCuboidDimensions();

    Cube getParentCube();

    /**
     * Get cuboids' hbase table name.
     * <P>
     * This rule will be used both in compiler and scanning client, so it better
     * be in the model domain.
     * 
     * @return cuboid's hbase table name.
     */
    String getCuboidTableName() throws IOException;

    void setTablePrefix(String prefix);

    /**
     * 
     * @return cuboid hbase table key length
     */
    int getKeyLen();

    /**
     * HBase cuboid table TTL attribute
     * 
     */
    int getHbaseTTL();

    /**
     * 
     * @return HBase cuboid table In-mem attribute
     */
    boolean isHbaseInMemory();

    /**
     * 
     * @return HBase cuboid table max versions attribute
     */
    int getHbaseMaxVersions();

    /**
     * We may also want to partition compilation runs by cuboids that they
     * build. Typically, we may want compile cuboids that have high aggregation
     * and more real time requirements, to run at finer time increments than
     * cuboids that deal of not so real time, low aggregation data during
     * off-peak hours. So we can assign compiler groups to include to a compiler
     * which generates the script and constrain cuboids being compiled at that
     * time to only those that participated in specified compiler groups.
     * 
     * @see Pig8CubeIncrementalCompilerBean#setCuboidGroupsInclude(java.util.Set)
     * 
     * @return compiler group cuboid belongs to
     */
    String getCompilerGroup();

}
