package com.inadco.math.aggregators;

import org.apache.hadoop.io.Writable;
/**
 * Generic online summarizer with general equipment such as reset, combine etc.  
 * 
 * @author dmitriy
 *
 * @param <T>
 */
public interface OnlineSummarizer<T extends OnlineSummarizer<T>> extends Writable {
    
    void reset();
    void assign(T other);
    void combine (T other);
    
    /**
     * Get main summarized parameter. 
     * Some summarizers count more than one parameter 
     * in which case see ad-hoc implementation. 
     * By default it is mean, average or rate. 
     * 
     * @return summarized value
     */
    double getValue();
    
    /**
     * Combine positive binomial history into something we counted as negative only history. 
     * This is an optional operation.<P> 
     * 
     * The use case in point: click logs and impression logs to arrive at click-thru rate metric. 
     * Realtime impression job counts all impressions as negative (x=0) samples. 
     * Click-true realtime job counts all clicks as positive-only history related to 
     * impression time.<P> 
     * 
     * Then we can't just combine those two because some impressions would be counted twice: 
     * once counted as negatives and once as having a click (positive) so the rate gets dilluted. 
     * The error probably would be small, but technically we can't do that. But luckily, 
     * we don't have to: most average tracking stuff can be combined that way by plussing 
     * history numerators while ignoring the history length of positive samples for negative 
     * history is a superset. 
     * 
     * @param positiveHistory
     */
    void combineBinomialOnes(T positiveHistory);

}
