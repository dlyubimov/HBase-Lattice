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
         */
        if (group == null) {
            // special case: startup
            group = groupingStrategy.newGroupHolder(null);
            sortedDelegate.next();
            T item = sortedDelegate.current();
            groupingStrategy.initGroup(group, item);
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
                T item = sortedDelegate.current();
                groupingStrategy.initGroup(nextGroup, item);
                groupingStrategy.aggregate(nextGroup, item);
                current = group;
                group = nextGroup;
                atEnd = false;
                break;
            }
        }
        if (atEnd) {
            current = group;
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
