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

import java.util.Arrays;

import org.apache.commons.lang.Validate;

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
    protected int    keylen;

    public HexDimension(String name, int keylen) {
        super(name);
        this.name = name;
        this.keylen = keylen;
    }

    @Override
    public int getKeyLen() {
        return keylen << 1;
    }

    @Override
    public void getKey(Object member, byte[] buff, int offset) {

        byte[] key;

        if (member instanceof byte[]) {
            key = (byte[]) member;
            Validate.isTrue(key.length == keylen, "Wrong hex id length");
        } else if (member instanceof Number) {
            long keyL = ((Number) member).longValue();
            key = new byte[keylen];
            for (int i = keylen - 1; i >= 0; i++, keyL >>>= 8)
                key[i] = (byte) keyL; // BEndian
        } else if (member instanceof String) {
            // ok we assume hex representation in the string
            String keyStr = (String) member;
            int diff = 2 * keylen - keyStr.length();
            if (diff < 0)
                throw new IllegalArgumentException("supplied string too long");

            /*
             * just copy every character of the string, while validating and
             * canonizing
             */

            // left-pad
            if (diff > 0)
                Arrays.fill(buff, offset, diff, (byte) '0');
            offset += diff;
            // copy with validation and canonization
            for (int i = 0; i < keyStr.length(); i++) {
                char ch = Character.toUpperCase(keyStr.charAt(i));
                if (!(ch <= '9' && ch >= '0') && !(ch <= 'F' && ch >= 'A'))
                    throw new IllegalArgumentException(String.format("not a valid hex string: %s.", keyStr));
                buff[i + offset] = (byte) ch;
            }
            return; // return because we are already in hex form

        } else {
            
            /* 
             * Q: should we support null dimensions as 0x000000 or something?
             * or we should rely on preambula script to do the coercion?
             */
            Validate.isTrue(false, "unsupported type/null for a dimension member");
            return;
        }

        HblUtil.fillCompositeKeyWithHex(key, 0, keylen, buff, offset);
    }

    @Override
    public Object getMember(byte[] buff, int offset) {
        byte[] id = new byte[keylen];
        HblUtil.readCompositeKeyHex(buff, offset, id, 0, keylen);
        return id;
    }

}
