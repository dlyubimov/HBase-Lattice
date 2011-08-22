package com.inadco.hbl.api;

public interface Hierarchy extends Dimension {

    // currently, no use case for as per below.
    // in theory, it probably makes sense for parsed queries such as in MDX
    // where
    // each hierarchy level has a distinct label, but we did not get there yet,
    // so
    // commented out till then.

    // void getKey(Object[] members, byte[] buff, int offset );

    /**
     * Another variation for the key evaluation in addition to
     * {@link Dimension#getKey(Object, byte[], int)} that also takes into
     * account maximum depth of evaluation.
     * <P>
     * 
     * i.e. in case of [ALL].[year-month].[date-hh] hierarcy, hearchyDepth=0
     * corresponds to [ALL] key, 1 corresponds to [year-month].[ALL] key, 2
     * corresponds to [year-month].[date-hh] key.
     * <P>
     * 
     */
    void getKey(Object member, int hierarchyDepth, byte[] buff, int offset);

    /**
     * returns depth of hierarchy e.g. for time hierachy
     * [ALL].[year-month].[date-hour] there are 3 levels.<P>
     */
    int getDepth();
}
