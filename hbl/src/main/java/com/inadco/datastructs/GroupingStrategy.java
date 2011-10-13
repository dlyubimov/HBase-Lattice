package com.inadco.datastructs;

/**
 * grouping strategy for grouping tuples of type T and group tuples of type G.
 * <P>
 * 
 * @author dmitriy
 * 
 * @param <G> group type
 * @param <T> tuple(item) type
 */

public interface GroupingStrategy<G,T> {

    /**
     * Test if next item belongs to a group group.
     * @param group
     * @param item
     * @return true if item is in the group.
     */
    boolean isItemInGroup(G group, T item);

    /**
     * aggregate data part of the item into the group tuple data. 
     * 
     * @param group
     * @param next
     */
    void aggregate(G group, T next);

    /**
     * create a new group holder (or reset an old one if supplied)
     * 
     * @param old
     *            optinally could supply an unneeded old group object for
     *            recycling
     * @return
     */
    G newGroupHolder(G old);

}
