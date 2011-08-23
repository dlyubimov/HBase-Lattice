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
package com.inadco.hbl.model;

import com.inadco.hbl.util.HblUtil;

/**
 * This is a dimension implementation for stuff like byte array ids or hashcodes
 * which is a recommended technique for hbase keys.
 * <P>
 * 
 * It resolves into double-length hexadecimal keys in hbase, e.g. 16-byte MD5
 * would translate into 32-byte key. Not very economic, but we have to use it
 * for convenience while debugging using hbase shell.
 * <P>
 * 
 * @author dmitriy
 * 
 */

public class HexDimension extends AbstractDimension {
    protected String name;
    protected int keylen;


    public HexDimension(String name, int keylen) {
        super(name);
        this.name = name;
        this.keylen = keylen;
    }

    @Override
    public int getKeyLen() {
        return keylen<<1;
    }

    @Override
    public void getKey(Object member, byte[] buff, int offset) {
        
        byte[] key=(byte[]) member;
        HblUtil.fillCompositeKeyWithHex(key, 0, keylen, buff, offset);
    }

    

}
