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
package com.inadco.datastructs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.inadco.hbl.util.IOUtil;


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
public class BlockingPipe<T> implements Closeable {

    private BlockingQueue<Object> queue;

    private static final Object   EOF        = new Object();

    private AtomicBoolean         closed     = new AtomicBoolean(false);
    private Deque<Closeable>      closeables = new ArrayDeque<Closeable>();

    public BlockingPipe(int queueCapacity) {
        super();

        queue = new ArrayBlockingQueue<Object>(queueCapacity);
        closeables.addFirst(input);
        closeables.addFirst(output);
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
    
    @Override
    public void close() throws IOException {
        try {
            IOUtil.closeAll(closeables);
        } finally { 
            closed.set(true);
        }
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
