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

/**
 * grouping strategy for grouping tuples of type T and group tuples of type G.
 * <P>
 * 
 * @author dmitriy
 * 
 * @param <G>
 *            group type
 * @param <T>
 *            tuple(item) type
 */

public interface GroupingStrategy<G, T> {

    /**
     * Test if next item belongs to a group group.
     * 
     * @param group
     * @param item
     * @return true if item is in the group.
     */
    boolean isItemInGroup(G group, T item);

    /**
     * init group attributes from new group item
     * 
     * @param group
     * @param item
     */
    void initGroup(G group, T item);

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
