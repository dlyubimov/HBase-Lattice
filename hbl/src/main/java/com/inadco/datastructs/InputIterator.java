package com.inadco.datastructs;

import java.io.Closeable;
import java.io.IOException;

/**
 * Abstraction of a streaming input strategy producing objects.<P>
 * 
 *  Very similar to iterator except we don't have to access current object 
 *  just one time<P>
 *  
 *  Plus we also assume I/O behind the scenes, so we propagate IOException as 
 *  a checked exception (debatable) <P> 
 * 
 * @author lyubimovd
 *
 */
public interface InputIterator<T> extends Closeable {
    
    /**
     * 
     * @return true if has more elements in the stream, 
     * false if at the end
     */
    boolean hasNext() throws IOException ;

    /**
     * go to the next element in the input
     *  
     */
    void next() throws IOException ;
    
    /**
     * 
     * @return current input element 
     */
    T current() throws IOException;
    
    
    int getCurrentIndex() throws IOException;
    

}
