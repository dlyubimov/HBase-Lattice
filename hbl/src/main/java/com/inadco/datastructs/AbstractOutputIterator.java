package com.inadco.datastructs;

import java.io.IOException;

/**
 * Abstract output sink of indefinite length that by default ignores all items
 * that are passed in.
 * 
 * @author dmitriy
 * 
 * @param <T>
 */
public abstract class AbstractOutputIterator<T> implements OutputIterator<T> {

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean hasNext() throws IOException {
        return true;
    }

    @Override
    public void add(T item) throws IOException {

    }

}
