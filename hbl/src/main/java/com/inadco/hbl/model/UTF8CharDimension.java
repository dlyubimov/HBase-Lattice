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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.client.HblException;
import com.inadco.hbl.util.IOUtil;

/**
 * Due to popular demand... character type dimension.
 * <P>
 * 
 * Normally it should probably still be converted into {@link HexDimension} due
 * to the fact that queries over character values may be non-uniformly
 * distributed, so in order to support a good uniform query distribution to
 * hbase cluster running hbl, it better be uniformly distributed thru hashing.
 * <P>
 * 
 * Of course the problem is that as of the time of this writing, dimensions
 * don't support properties, or reverse lookups to go from hashed value back to
 * character value. so direct character keys may make some sense still. The
 * problem of non-uniform query distribution also may be not so important in
 * some use cases.
 * <P>
 * 
 * The storage considerations for this type are similar to that of RDBMS' CHAR()
 * type. i.e. since the values are part of the key index, they are of fixed
 * maximum size and the space requested will always be used even if string is
 * short than that. Because of that, enabling hbase compression is probably a
 * good idea. YMMV.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class UTF8CharDimension extends AbstractDimension {

    private static final Charset utf8 = Charset.forName("utf-8");

    private int                  len;
    private boolean              autoTruncateFacts;
    private Object               nullkey;

    public UTF8CharDimension(String name, int len) {
        this(name, len, true, null);
    }

    public UTF8CharDimension(String name, int len, boolean autoTruncateFacts) {
        this(name, len, autoTruncateFacts, null);
    }

    public UTF8CharDimension(String name, int len, boolean autoTruncateFacts, Object nullkey) {
        super(name);
        Validate.isTrue(len > 0);
        this.len = len;
        this.autoTruncateFacts = autoTruncateFacts;
        this.nullkey = nullkey == null ? null : IOUtil.tryClone(nullkey);
    }

    @Override
    public int getKeyLen() {
        return len;
    }

    @Override
    public void getKey(Object member, byte[] buff, int offset) {

        if (member == null) {
            member = nullkey;
        }

        if (member instanceof CharSequence) {
            CharSequence cs = (CharSequence) member;
            ByteBuffer bb = utf8.encode(cs.toString());

            int mlen = bb.remaining();
            if (mlen > len) {
                if (autoTruncateFacts)
                    mlen = len;
                else
                    throw new IllegalArgumentException(
                        String.format("fact length exceeds type length for dimension %s.", name));
            }
            bb.get(buff, offset, mlen);
            Arrays.fill(buff, offset + mlen, offset + len, (byte) 0);
        } else {
            throw new IllegalArgumentException(String.format("unsupported type/null for a member of dimension %s.",
                                                             name));
        }

    }

    @Override
    public Object getMember(byte[] buff, int offset) throws HblException {
        int l = len - 1;
        for (; l >= 0; l--) {
            if (buff[offset + l] != (byte) 0x0)
                break;
        }

        CharBuffer cb = utf8.decode(ByteBuffer.wrap(buff, offset, ++l));
        return cb.toString();
    }

}
