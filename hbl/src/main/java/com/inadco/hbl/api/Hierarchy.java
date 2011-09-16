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
    
    /**
     * Test the key depth coming in a scan. 
     * 
     * @param buff hierarchy key coming in a scan. 
     * 
     * @param offset offset of the key
     * 
     * @return key hierarchy depth corresponding to this key.
     */
    int keyDepth(byte[] buff, int offset );
}
