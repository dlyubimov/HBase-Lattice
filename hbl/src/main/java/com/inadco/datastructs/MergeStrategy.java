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

import java.io.IOException;

import com.inadco.datastructs.util.NWayMerge;

/**
 * 
 * Merge strategy abstraction. (Not all merges have to be sort merges. Think about it -:)  
 * 
 * @author dmitriy
 * 
 * @param <T>
 */
public interface MergeStrategy<T> {

    /**
     * Step instructions that contain merge step outcome for {@link NWayMerge}.
     * 
     */
    public static class StepResult {
        /**
         * 
         * indexes in the inputs to output. We, generally, assume that this
         * should contain only distinct values.
         * 
         * This is preallocated in call to
         * {@link MergeStrategy#processStep(InputIterator[], StepResult)}, so no
         * need to re-allocate.
         */
        public int[] m_output;
        /**
         * should be set to the number of valid indexes in the
         * {@link MergeStrategy.StepResult#m_output}.
         * <P>
         * 
         * This should be >=0.
         * 
         */
        public int   m_outputCount;
        /**
         * indexes in the inputs that is to advance after this step.
         * <P>
         * 
         * This is preallocated in call to
         * {@link MergeStrategy#processStep(InputIterator[], StepResult)}, so no
         * need to re-allocate.
         */
        public int[] m_advance;
        /**
         * should be set to the number of valid indexes in the
         * {@link MergeStrategy.StepResult#m_advance}.
         * <P>
         * 
         * This should be >=1 (at least one input should be advanced as a result
         * of merge iteration, otherwise merge would loop indefinitely.
         * 
         * 
         */
        public int   m_advanceCount;

    }

    /**
     * 
     * @param inputs
     *            the inputs array for this merge step. This always contains all
     *            original inputs in the same order, even if some of inputs are
     *            already consumed. In addition, the empty inputs are going to
     *            be pased in as <code>null</code>s.
     * 
     * @param result
     *            Further instructions for the merge should be saved there. See
     *            also {@link MergeStrategy.StepResult} for details.
     * @throws IOException
     */
    void processStep(InputIterator<? extends T>[] inputs, StepResult result) throws IOException;

}
