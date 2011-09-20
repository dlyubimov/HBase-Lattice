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
package com.inadco.datastructs.util;

import java.io.IOException;
import java.util.Comparator;

import com.inadco.datastructs.InputIterator;
import com.inadco.datastructs.MergeStrategy;

/**
 * a standard stock N-way sort merge strategy.
 * <P>
 * 
 * It is also stateful (i.e. internal state is maintained for the same sort, so
 * it's not safe to use it for several merges simultaneously, either in a
 * thread-safe way or not.
 * <P>
 * 
 * Can be re-used, but {@link #reset()} must be called between uses.
 * <P>
 * 
 * Will perform at O(log(N)) in the worst case per merging iteration ( assuming
 * M*N is signifantly greater than 1), where N is the number of sorted inputs.
 * Thus, total complexity of N-way merge with this strategy would become
 * O(M*N*log(N)), where N is the number of inputs, and M is the average input
 * size (thus, total # of elements is approx. M*N).
 * <P>
 * 
 * @param <T>
 *            element type
 * 
 * @author dmitriy
 * 
 */
public class StatefulHeapSortMergeStrategy<T> implements MergeStrategy<T> {

    private int[]                 m_heap;
    private Comparator<? super T> m_comparator;

    public StatefulHeapSortMergeStrategy() {
        this(null);
    }

    public StatefulHeapSortMergeStrategy(Comparator<? super T> comparator) {
        m_comparator = comparator;
        if (m_comparator == null)

            m_comparator = new Comparator<Object>() {
                @Override
                @SuppressWarnings("unchecked")
                public int compare(Object o1, Object o2) {
                    assert o1 instanceof Comparable<?>;
                    assert o2 instanceof Comparable<?>;
                    return ((Comparable<Object>) o1).compareTo(o2);
                }

            };
        reset();

    }

    public void reset() {
        m_heap = null;
    }

    @Override
    public void processStep(InputIterator<? extends T>[] inputs, MergeStrategy.StepResult result) throws IOException {
        if (m_heap == null)
            buildHeap(inputs);
        else
            siftThru(0, m_heap.length, inputs);

        // always output and advance the top of the heap.
        result.m_outputCount = 1;
        result.m_output[0] = m_heap[0];
        result.m_advanceCount = 1;
        result.m_advance[0] = m_heap[0];
    }

    private void buildHeap(InputIterator<? extends T>[] inputs) throws IOException {
        m_heap = new int[inputs.length];
        for (int i = 0; i < inputs.length; i++)
            m_heap[i] = i;
        _heapify(inputs);

    }

    private void _heapify(InputIterator<? extends T>[] inputs) throws IOException {
        for (int start = (m_heap.length >>> 1) - 1; start >= 0; start--)
            siftThru(start, m_heap.length, inputs);
    }

    private void siftThru(int start, int len, InputIterator<? extends T>[] inputs) throws IOException {
        for (int child1 = (start << 1) + 1; child1 < len; start = child1, child1 = (start << 1) + 1) {

            if (child1 + 1 < len) {
                int child2 = child1 + 1;
                // min-heap, right?
                if (inputs[m_heap[child1]] == null || inputs[m_heap[child2]] != null
                    && m_comparator.compare(inputs[m_heap[child1]].current(), inputs[m_heap[child2]].current()) > 0)
                    child1 = child2;
            }
            if (inputs[m_heap[start]] == null
                || (inputs[m_heap[child1]] != null && m_comparator.compare(inputs[m_heap[start]].current(),
                                                                           inputs[m_heap[child1]].current()) > 0)) {
                int swap = m_heap[start];
                m_heap[start] = m_heap[child1];
                m_heap[child1] = swap;
            } else
                break;
        }
    }
}
