package com.inadco.hbl.api;

import java.io.IOException;
import java.util.List;

/**
 * Cuboid api
 * 
 * @author dmitriy
 * 
 */
public interface Cuboid {

    List<String> getCuboidPath();

    List<Dimension> getCuboidDimenions();

    /**
     * Get cuboids' hbase table name.
     * <P>
     * This rule will be used both in compiler and scanning client, so it better
     * be in the model domain.
     * 
     * @return cuboid's hbase table name.
     */
    String getCuboidTableName() throws IOException;
    
    void setTablePrefix ( String prefix );
    
    int getKeyLen();
    
    // hbase attributes: TTL,
    int getHbaseTTL() ;

    boolean isHbaseInMemory() ;

    int getHbaseMaxVersions() ;

}
