package com.inadco.datastructs;

import java.io.Closeable;
import java.io.IOException;

/**
 * Output streaming iterator, counterpart for  
 * {@link InputIterator} for abstracting streaming, 
 * or defining output strategies.
 * 
 * @author lyubimovd
 *
 */
public interface OutputIterator<T> extends Closeable {

    boolean hasNext() throws IOException;
    void    add(T item) throws IOException;
    
}
