package com.inadco.datastructs.adapters;

import java.io.IOException;

import com.inadco.datastructs.GroupingStrategy;
import com.inadco.datastructs.InputIterator;

/**
 * Grouping iterator that assumes the stream comes with grouped tuples following
 * one each other (which usually means they are sorted by the group key). Then
 * combines every group into one using {@link GroupingStrategy}.
 * <P>
 * 
 * {@link GroupingStrategy} plays the roles of group tuple factory AND group
 * combiner.
 * <P>
 * 
 * 
 * @author dmitriy
 * 
 * @param <G>
 *            group
 * @param <T>
 *            item
 */
public class GroupingIterator<G, T> implements InputIterator<G> {

    private InputIterator<? extends T>     sortedDelegate;
    private GroupingStrategy<G, ? super T> groupingStrategy;
    private int                            currentIndex = -1;

    private G                              group, current;

    public GroupingIterator(InputIterator<? extends T> sortedDelegate, GroupingStrategy<G, ? super T> groupingStrategy) {
        super();
        this.sortedDelegate = sortedDelegate;
        this.groupingStrategy = groupingStrategy;
    }

    public void close() throws IOException {
        sortedDelegate.close();
    }

    public boolean hasNext() throws IOException {
        if (group == null)
            return sortedDelegate.hasNext();
        return true;
    }

    public void next() throws IOException {
        if (!hasNext())
            throw new IOException("iterator at the end");
        currentIndex++;

        /*
         * bootstrap first tuple into the group if were not bootstrapped earlier
         * 
         */
        if (group == null) {
            // special case: startup
            group = groupingStrategy.newGroupHolder(null);
            sortedDelegate.next();
            groupingStrategy.aggregate(group, sortedDelegate.current());
        }

        // flush the group
        boolean atEnd = true;
        while (sortedDelegate.hasNext()) {
            sortedDelegate.next();
            if (groupingStrategy.isItemInGroup(group, sortedDelegate.current())) {
                groupingStrategy.aggregate(group, sortedDelegate.current());
            } else {
                G nextGroup = groupingStrategy.newGroupHolder(current);
                groupingStrategy.aggregate(nextGroup, sortedDelegate.current());
                current = group;
                group = nextGroup;
                atEnd = false;
                break;
            }
        }
        if (atEnd) { 
            current=group;
            group = null;
        }
    }

    public G current() throws IOException {
        return current;
    }

    public int getCurrentIndex() throws IOException {
        return currentIndex;
    }

}
