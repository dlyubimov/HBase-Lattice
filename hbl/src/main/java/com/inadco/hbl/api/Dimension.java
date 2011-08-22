package com.inadco.hbl.api;


public interface Dimension {
    
    String getName();
    int getKeyLen();
    
    void getKey(Object member, byte[] buff, int offset );
    

}
