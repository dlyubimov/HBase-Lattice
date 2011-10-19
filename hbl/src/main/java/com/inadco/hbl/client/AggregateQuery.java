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
package com.inadco.hbl.client;

/**
 * Aggregate query interface.
 * <P>
 * 
 * Major capabilities:
 * <UL>
 * 
 * <LI>define slice ({@link #addHalfOpenSlice(String, Object, Object)}, etc. The
 * slice specifications are consistent with mathematical definition of closed
 * and open intervals. Closed boundary means the boundary value is included into
 * the result. Open boundary beans it is not included. Unbounded queries are
 * implicitly supported thru putting closed boundary with maximum value possible
 * for that particular data type. <B>IMPORTANT</B> at this time of this writing,
 * only one range per dimension is supported. If another range is added for the
 * same dimension, it would just simply overwrite the previous one. There's no
 * technical reason not to support multiple slices on the same query except that
 * it is just did not happen to be common case enough to have immediate support
 * for. If multiple slices are needed, they could always be obtained by running
 * a query per slice and then aggregating the results manuallly at this pont.
 * 
 * 
 * <LI>define projection ({@link #addGroupBy(String)}. Note that unlike with
 * RDBMS, there's never a query without projections. If you don't specify any
 * dimension for grouping, it assumes full group projection rather than access
 * to individual facts (i.e. such query will always return only one tuple
 * corresponding to the entire slice). Access to individual facts from original
 * fact stream is in fact not possible, as we never ever save individual facts
 * anywhere. This project will assume one could obtain individual facts from
 * some other system of record.
 * 
 * <LI>define desired measures to be output ({@link #addMeasure(String)}.
 * 
 * </UL>
 * <P>
 * 
 * Note that query initialization is relatively expensive. If you use HBL system
 * table to pull cube model definitions from, then this will be executed during
 * AggregateQuery construction. To make sure there's best performance, try to
 * reuse query objects, with help of {@link #reset()}.
 * <P>
 * 
 * This contracts also assume that actual implementation is non-reentrant (not
 * thread safe).
 * 
 * @author dmitriy
 * 
 */
public interface AggregateQuery {

    /**
     * Add measure to compile group operators for.
     * 
     * @param measure
     *            the measure name.
     * @return self
     */
    AggregateQuery addMeasure(String measure);

    /**
     * Add a projection (group specification)
     * 
     * @param dimName
     *            dimension to be added as coordinate of projection
     * @return self
     */
    AggregateQuery addGroupBy(String dimName);

    /**
     * adds closed range (interval) specification for a slice for a given
     * dimension. Only at most one range specification can be added for
     * individual projection at this time.
     * 
     * @param dimension
     *            dimension name to slice upon.
     * 
     * @param leftBound
     *            left bound of the dimension range.
     * @param rightBound
     *            right bound of the dimension range.
     * @return
     */
    AggregateQuery addClosedSlice(String dimension, Object leftBound, Object rightBound);

    /**
     * add open slice based on dimension.
     * 
     * @param dimension
     * @param leftBound
     * @param rightBound
     * @return
     */
    AggregateQuery addOpenSlice(String dimension, Object leftBound, Object rightBound);

    AggregateQuery addHalfOpenSlice(String dimension, Object leftBound, Object rightBound);

    /**
     * 
     * @param dimension
     *            dimension to add
     * @param leftBound
     *            left bound value, null if unbounded
     * @param leftOpen
     * @param rightBound
     *            right bound value, null if unbounded
     * @param rightOpen
     * @return
     */
    AggregateQuery addSlice(String dimension, Object leftBound, boolean leftOpen, Object rightBound, boolean rightOpen);

    /**
     * Execute the query
     * 
     * @return result set . It is expected to be closed by the user!
     * @throws HblException
     */
    AggregateResultSet execute() throws HblException;

    /**
     * reset for re-use
     */
    void reset();
}
