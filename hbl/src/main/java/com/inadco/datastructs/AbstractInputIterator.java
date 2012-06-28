package com.inadco.datastructs;

import java.io.IOException;

/**
 * Abstract iterator (by default empty iterator that does nothing).
 * 
 * @author dmitriy
 * 
 * @param <T>
 */
public abstract class AbstractInputIterator<T> implements InputIterator<T> {

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public void next() throws IOException {

    }

    @Override
    public T current() throws IOException {
        throw new IOException("After last position in iterator.");
    }

    @Override
    public int getCurrentIndex() throws IOException {
        return 0;
    }

}
