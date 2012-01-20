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
package com.inadco.datastructs.adapters;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import com.inadco.datastructs.InputIterator;
import com.inadco.datastructs.MergeStrategy;
import com.inadco.datastructs.util.NWayMerge;
import com.inadco.hbl.util.IOUtil;

/**
 * Iterator version of {@link NWayMerge} util
 * <P>
 * 
 * @author dmitriy
 * 
 * @param <T>
 */

public class NWayMergingIterator<T> implements InputIterator<T> {

    private Deque<Closeable>             m_closeables   = new ArrayDeque<Closeable>();
    private InputIterator<? extends T>[] m_inputs;
    private MergeStrategy<T>             m_mergeStrategy;
    private int                          m_currentIndex = -1;
    private MergeStrategy.StepResult     m_stepResult   = new MergeStrategy.StepResult();
    private Set<Integer>                 m_unfinished;
    private int                          m_stepOutputIdx;                                // index
                                                                                          // of
                                                                                          // output
                                                                                          // for
                                                                                          // current
                                                                                          // step.

    public NWayMergingIterator(InputIterator<? extends T>[] inputs, MergeStrategy<T> mergeStrategy, boolean cascadeClose)
        throws IOException {
        super();
        m_inputs = inputs;
        m_mergeStrategy = mergeStrategy;
        if (cascadeClose)
            m_closeables.addAll(Arrays.asList(inputs));
        m_unfinished = new HashSet<Integer>((inputs.length << 1) + 1, 0.75f);
        m_stepResult.m_advance = new int[inputs.length];
        m_stepResult.m_output = new int[inputs.length];

        // we assume all inputs here are before their first position, so we try
        // advance all just this one time
        for (int i = 0; i < inputs.length; i++)
            if (inputs[i].hasNext()) {
                inputs[i].next();
                m_unfinished.add(i);
            } else {
                inputs[i] = null;
            }
    }

    @Override
    public void close() throws IOException {
        IOUtil.closeAll(m_closeables);
    }

    @Override
    public boolean hasNext() throws IOException {
        return m_stepOutputIdx + 1 < m_stepResult.m_outputCount || m_unfinished.size() > 0;
    }

    @Override
    public void next() throws IOException {
        m_currentIndex++;

        if (!hasNext())
            throw new IOException("at the end of the iterator.");

        m_stepOutputIdx++;

        if (m_stepResult.m_outputCount <= m_stepOutputIdx)
            while (m_stepResult.m_outputCount <= m_stepOutputIdx) {

                // advance the requested ones

                for (int i = 0; i < m_stepResult.m_advanceCount; i++) {
                    int advanceIndex = m_stepResult.m_advance[i];
                    assert advanceIndex >= 0 && advanceIndex < m_inputs.length;

                    if (m_unfinished.contains(advanceIndex))
                        m_inputs[advanceIndex].next();
                    else
                        m_inputs[advanceIndex] = null;
                }

                m_mergeStrategy.processStep(m_inputs, m_stepResult);
                m_stepOutputIdx = 0;

                assert m_stepResult.m_advanceCount >= 1 && m_stepResult.m_advanceCount <= m_inputs.length : "m_stepResult._advanceCount out of range: "
                    + m_stepResult.m_advanceCount;

                assert m_stepResult.m_outputCount >= 0 && m_stepResult.m_outputCount <= m_inputs.length : "m_stepResult._outputCount out of range:"
                    + m_stepResult.m_outputCount;

                for (int i = 0; i < m_stepResult.m_advanceCount; i++) {
                    int advanceIndex = m_stepResult.m_advance[i];
                    if (!m_inputs[advanceIndex].hasNext()) {
                        m_unfinished.remove(advanceIndex);
                        m_mergeStrategy.onInputRemoved(m_inputs[advanceIndex]);
                    }
                }
                // for (int i = 0; i < m_stepResult.m_outputCount; i++)
                // output.add(m_inputs[m_stepResult.m_output[i]].current());
            }

    }

    @Override
    public T current() throws IOException {
        return m_inputs[m_stepResult.m_output[m_stepOutputIdx]].current();
    }

    @Override
    public int getCurrentIndex() throws IOException {
        return m_currentIndex;
    }
}
