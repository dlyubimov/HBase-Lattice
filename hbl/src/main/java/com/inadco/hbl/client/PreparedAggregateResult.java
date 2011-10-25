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
 * if you were Using {@link PreparedAggregateQuery} then this is the result
 * interface you can cast to. It provides additional capabilities of retrieving
 * query result by either index or alias specified in the query.
 * <P>
 * 
 * @author dmitriy
 * 
 */

public interface PreparedAggregateResult extends AggregateResult {

    /**
     * get result by alias
     * 
     * @param alias
     *            select expression alias
     * @return query select expression result
     * @throws HblException
     */
    Object getObject(String alias) throws HblException;

    /**
     * get result by index
     * 
     * @param index
     *            0-based index of expression in the select expression list
     * @return
     * @throws HblException
     */
    Object getObject(int index) throws HblException;

}
