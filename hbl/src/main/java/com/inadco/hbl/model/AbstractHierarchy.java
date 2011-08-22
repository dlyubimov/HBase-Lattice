package com.inadco.hbl.model;

import com.inadco.hbl.api.Hierarchy;

public abstract class AbstractHierarchy extends AbstractDimension implements Hierarchy {

    public AbstractHierarchy(String name, String[] hierarchyPath ) {
        super(name);
    }

    @Override
    public void getKey(Object member, byte[] buff, int offset) {
        // by default, just evaluate hierarchy at deepest level. 
        getKey(member,getDepth()-1,buff,offset);
    }

    

    
    
}
