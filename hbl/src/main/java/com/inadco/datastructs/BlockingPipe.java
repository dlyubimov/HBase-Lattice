package com.inadco.datastructs;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * connect asynchronous output from out there into synchronous input here.
 * 
 * Warning: objects flowing thru pipe are obviously not to be reused by the
 * writing head (such as writables in mappers, reducers). if that's the case
 * then the writing end should deep-clone elements being written, or perhaps
 * borrow them from a pool (to be released by the consuming end.)
 * 
 * @author dmitriy
 * 
 * @param <T>
 *            pipe element type
 */
public class BlockingPipe<T> {

    private BlockingQueue<Object> queue;

    private static final Object   EOF    = new Object();

    private AtomicBoolean         closed = new AtomicBoolean(false);

    BlockingPipe(int queueCapacity) {
        super();

        queue = new ArrayBlockingQueue<Object>(queueCapacity);
    }

    
    public static Object getEof() {
        return EOF;
    }

    public OutputIterator<T> getInput() {
        return input;
    }

    public InputIterator<T> getOutput() {
        return output;
    }


    private OutputIterator<T> input  = new OutputIterator<T>() {

                                         @Override
                                         public void close() throws IOException {
                                             try {
                                                 if (closed.get())
                                                     throw new IOException("Broken pipe");
                                                 queue.put(EOF);
                                             } catch (InterruptedException exc) {
                                                 throw new IOException("Interrupted", exc);
                                             }
                                         }

                                         @Override
                                         public boolean hasNext() throws IOException {
                                             return true;
                                         }

                                         @Override
                                         public void add(T item) throws IOException {
                                             try {
                                                 if (closed.get())
                                                     throw new IOException("Broken pipe");
                                                 queue.put(item);
                                             } catch (InterruptedException exc) {
                                                 throw new IOException("Interrupted", exc);
                                             }

                                         }

                                     };

    private InputIterator<T>  output = new InputIterator<T>() {

                                         private boolean eof          = false;
                                         private T       next;
                                         private T       current;
                                         private int     currentIndex = -1;

                                         @Override
                                         public void close() throws IOException {
                                             closed.set(true);
                                             // drain queue if writes are
                                             // pending so they have a chance to
                                             // cancel
                                             while (queue.poll() != null)
                                                 ;

                                         }

                                         @Override
                                         @SuppressWarnings("unchecked")
                                         public boolean hasNext() throws IOException {
                                             if (next != null)
                                                 return true;
                                             if (eof)
                                                 return false;
                                             Object next = queue.poll();
                                             if (next == EOF) {
                                                 eof = true;
                                                 return false;
                                             }
                                             this.next = (T) next;
                                             return true;
                                         }

                                         @Override
                                         public void next() throws IOException {
                                             if (hasNext()) {
                                                 current = next;
                                                 next = null;
                                                 currentIndex++;
                                             }
                                         }

                                         @Override
                                         public T current() throws IOException {
                                             if (current == null)
                                                 throw new IOException("iterator not positioned yet/already");
                                             return current;
                                         }

                                         @Override
                                         public int getCurrentIndex() throws IOException {
                                             return currentIndex;
                                         }

                                     };

}
