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
 * Prepared query. The one which you can use by specifying sql-like 
 * query with parameters instead of constructing the query declaratively 
 * via {@link AggregateQuery} api. <P>
 * 
 * @author dmitriy
 *
 */
public interface PreparedAggregateQuery extends AggregateQuery {
    
    void prepare ( String statement ) throws HblException;
    void setHblParameter(int param, Object value) throws HblException;

}
