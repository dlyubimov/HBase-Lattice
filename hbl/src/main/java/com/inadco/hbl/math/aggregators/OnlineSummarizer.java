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
package com.inadco.hbl.math.aggregators;

import org.apache.hadoop.io.Writable;

/**
 * Generic online summarizer with general equipment such as reset, combine etc.
 * 
 * @author dmitriy
 * 
 * @param <T>
 */
public interface OnlineSummarizer<T extends OnlineSummarizer<T>> extends Writable, Cloneable {

    void reset();

    void assign(T other);

    void combine(T other);

    /**
     * 
     * if S1 ('this'), S2 ('other') are sets of events {x,t} such that S1
     * \supseteq S2, then this operation returns (adjusts) this as result of
     * sum( S1 \ S2 ).
     * <P>
     * 
     * Optional operation.
     * <P>
     * 
     * Also, asserts this.t >= other.t (since 'other' is supposed to correspond
     * to a subset of 'this'). This may cause problems with inconsistently taken
     * measurements, i.e. both things must probably be a result of a consistent
     * read, or consistency must otherwise be ensured by sacrificing some of
     * precision (artificialStretch=true).
     * <P>
     * 
     */
    void complement(T other, boolean artificialStretch);

    /**
     * Get main summarized parameter. Some summarizers count more than one
     * parameter in which case see ad-hoc implementation. By default it is mean,
     * average or rate.
     * 
     * @return summarized value
     */
    double getValue();

    /**
     * Combine positive binomial history into something we counted as negative
     * only history. This is an optional operation.
     * <P>
     * 
     * The use case in point: click logs and impression logs to arrive at
     * click-thru rate metric. Realtime impression job counts all impressions as
     * negative (x=0) samples. Click-true realtime job counts all clicks as
     * positive-only history related to impression time.
     * <P>
     * 
     * Then we can't just combine those two because some impressions would be
     * counted twice: once counted as negatives and once as having a click
     * (positive) so the rate gets dilluted. The error probably would be small,
     * but technically we can't do that. But luckily, we don't have to: most
     * average tracking stuff can be combined that way by plussing history
     * numerators while ignoring the history length of positive samples for
     * negative history is a superset.
     * 
     * @param positiveHistory
     */
    void combineBinomialOnes(T positiveHistory);

}
