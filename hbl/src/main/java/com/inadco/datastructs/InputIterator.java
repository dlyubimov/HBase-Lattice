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

/**
 * Abstraction of a streaming input strategy producing objects.
 * <P>
 * 
 * Very similar to iterator except we don't have to access current object just
 * one time
 * <P>
 * 
 * Plus we also assume I/O behind the scenes, so we propagate IOException as a
 * checked exception (debatable)
 * <P>
 * 
 * @author lyubimovd
 * 
 */
public interface InputIterator<T> extends Closeable {

    /**
     * 
     * @return true if has more elements in the stream, false if at the end
     */
    boolean hasNext() throws IOException;

    /**
     * go to the next element in the input
     * 
     */
    void next() throws IOException;

    /**
     * 
     * @return current input element
     */
    T current() throws IOException;

    int getCurrentIndex() throws IOException;

}
