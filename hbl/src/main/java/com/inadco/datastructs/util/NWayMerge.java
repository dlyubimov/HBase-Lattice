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
import java.util.HashSet;
import java.util.Set;

import com.inadco.datastructs.InputIterator;
import com.inadco.datastructs.MergeStrategy;
import com.inadco.datastructs.OutputIterator;

/**
 * N-way merging of streams using merge strategy and output strategy.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class NWayMerge {

    /**
     * Merge algorithm with default strategy.
     * <P>
     * 
     * As of this time, default merge strategy is
     * {@link StatefulHeapSortMergeStrategy}, which means default merge behavior
     * is non-deduplicating sorting merge.
     * <P>
     * 
     * @param <T>
     *            element type
     * @param inputs
     *            inputs
     * @param output
     *            merge output sink
     * @throws IOException
     *             on input or output IO
     */
    public static <T> void sortedMerge(InputIterator<? extends T>[] inputs, OutputIterator<? super T> output)
        throws IOException {
        merge(inputs, output, new StatefulHeapSortMergeStrategy<T>());
    }

    /**
     * Merge algorithm.
     * <P>
     * 
     * @param <T>
     *            type merge strategy can work with
     * @param inputs
     *            The set of inputs for the merge
     * @param output
     *            The merge output sink
     * @param mergeStrategy
     *            Applicable merge strategy
     * @throws IOException
     */
    public static <T> void merge(InputIterator<? extends T>[] inputs,
                                 OutputIterator<? super T> output,
                                 MergeStrategy<T> mergeStrategy) throws IOException {

        if (inputs.length == 0)
            return; // corner case: no inputs

        final Set<Integer> finished = new HashSet<Integer>((inputs.length << 1) + 1, 0.75f);
        final MergeStrategy.StepResult stepResult = new MergeStrategy.StepResult();
        stepResult.m_advance = new int[inputs.length];
        stepResult.m_output = new int[inputs.length];

        // we assume all inputs here are before their first position, so we try
        // advance all just this one time
        for (int i = 0; i < inputs.length; i++)
            if (inputs[i].hasNext())
                inputs[i].next();
            else {
                finished.add(i);
                inputs[i] = null;
            }

        while (finished.size() < inputs.length) {
            mergeStrategy.processStep(inputs, stepResult);

            // 1. output the requested ones
            assert stepResult.m_outputCount >= 0 && stepResult.m_outputCount <= inputs.length : "stepResult._outputCount out of range:"
                + stepResult.m_outputCount;

            for (int i = 0; i < stepResult.m_outputCount; i++)
                output.add(inputs[stepResult.m_output[i]].current());

            // 2. advance the requested ones
            assert stepResult.m_advanceCount >= 1 && stepResult.m_advanceCount <= inputs.length : "stepResult._advanceCount out of range: "
                + stepResult.m_advanceCount;

            for (int i = 0; i < stepResult.m_advanceCount; i++) {
                int advanceIndex = stepResult.m_advance[i];
                assert advanceIndex >= 0 && advanceIndex < inputs.length;

                if (inputs[advanceIndex].hasNext())
                    inputs[advanceIndex].next();
                else {
                    finished.add(advanceIndex);
                    inputs[advanceIndex] = null;
                }

            }
        }
    }

}
