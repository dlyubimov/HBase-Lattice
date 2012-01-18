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

import org.apache.hadoop.io.RawComparator;

import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.impl.Slice;

public interface Dimension {

    String getName();

    int getKeyLen();

    /**
     * convert java member type object into hbase key.
     * 
     * 
     * @param member
     *            java type this dimension supports. Hierarchies must support
     *            {@link HierarchyMember} as well.
     * @param buff
     * @param offset
     */
    void getKey(Object member, byte[] buff, int offset);

    /**
     * inverse conversion from an hbase represenetation to the member
     * presentation (whatever it might be).
     * 
     * @param buff
     *            the hbase value buffer
     * @param offset
     *            offset of the member int the hbase key
     * @return the dimension/hierarchy member
     * @throws HblException 
     */
    Object getMember(byte[] buff, int offset) throws HblException;

    RawComparator<?> getMemberComparator();

    Range[] optimizeSliceScan(Slice slice, boolean allowComplements);
    
    Range allRange ();

}
