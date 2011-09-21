package com.inadco.hbl.api;

public class HierarchyMember {
    private Object member;
    private int depth;
    
    public HierarchyMember(Object member, int depth) {
        super();
        this.member = member;
        this.depth = depth;
    }

    public Object getMember() {
        return member;
    }

    public int getDepth() {
        return depth;
    }
    
    

}
