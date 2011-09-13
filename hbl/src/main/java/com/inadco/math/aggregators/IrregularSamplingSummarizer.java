package com.inadco.math.aggregators;

/**
 * Summarizer that takes time of the sample taking into account (as in
 * {@link #update(double, double)} method).
 * <P>
 * 
 * @author dmitriy
 * 
 * @param <T> concrete summarizer type this summarizer can {@link #combine(OnlineSummarizer)} with.
 */
public interface IrregularSamplingSummarizer extends OnlineSummarizer<IrregularSamplingSummarizer> {

    /**
     * update current summarizer state with new observation.
     * <P>
     * 
     * @param x
     *            the value of observation
     * @param t
     *            the time of observation x
     * @return updated value
     */
    double update(double x, double t);

    /**
     * Some summarizers may maintain a notion that it matters how much it has
     * passed since last observation and adjust the estimate based on that.
     * (currently, only binomial summarizers can produced biased estimate based
     * on that).
     * <P>
     * 
     * @param tNow
     *            time "now"
     * @return biased estimate based on time passed between 'now' time and last
     *         known observation.
     */
    double getValueNow(double tNow);

}
