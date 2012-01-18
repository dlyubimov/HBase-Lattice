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
package com.inadco.hbl.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Cuboid;

/**
 * Various hbl helpers .
 * 
 * @author dmitriy
 * 
 */
public class HblUtil {

    static char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    /**
     * insert byte sources as hex representation into hbase composite key at a
     * given offset.
     * 
     * @param srcBytes
     *            src bytes to insert
     * @param srcOffset
     *            offset in src bytes
     * @param srcLen
     *            src length
     * @param holder
     *            target composite key
     * @param holderOffset
     *            offset in the target composite key
     * @return composite key (same as holder)
     */
    public static byte[] fillCompositeKeyWithHex(byte[] srcBytes,
                                                 int srcOffset,
                                                 int srcLen,
                                                 byte[] holder,
                                                 int holderOffset) {
        for (int i = 0; i < srcLen; i++) {
            holder[holderOffset++] = (byte) (hexArray[(srcBytes[srcOffset] & 0xf0) >>> 4]);
            holder[holderOffset++] = (byte) (hexArray[srcBytes[srcOffset++] & 0xf]);
        }
        return holder;
    }

    /**
     * test whether string is a valid hex representation of a byte array key of
     * length <code>keylen</code> Not terribly efficient, so probably use is not
     * encouraged.
     * 
     * @param hexString
     * @param keylen
     * @return
     */
    public static boolean isValidIDInHex(String hexString, int keylen) {

        if (hexString == null)
            return false;
        hexString = hexString.trim();
        if (hexString.length() == 0)
            return false;

        int l = hexString.length();
        int unpaddedLen = 0;
        for (int i = 0; i < l; i++) {
            char c = hexString.charAt(i);
            int v;
            if ((v = Character.digit(c, 16)) == -1)
                return false;
            if (v > 0 || unpaddedLen > 0)
                unpaddedLen++;
        }
        return (unpaddedLen + 1 >> 1) > keylen ? false : true;
    }

    /**
     * read hex-encoded part of composite key into holder
     * 
     * @param composite
     *            the src composite key
     * @param offset
     *            offset in the src composite key to start reading from
     * @param holder
     *            the buffer to read bytes into
     * @param holderOffset
     *            offset in the holder to read the bytes into
     * @param holderLen
     *            number of bytes to read into holder
     * @return
     */
    public static byte[] readCompositeKeyHex(byte[] composite,
                                             int offset,
                                             byte[] holder,
                                             int holderOffset,
                                             int holderLen) {

        for (int i = 0; i < holderLen; i++) {
            int c1 = composite[offset++] & 0xff, c2 = composite[offset++] & 0xff;
            c1 = (c1 - '0' < 10) ? c1 - '0' : c1 - 'A' + 10;
            c2 = (c2 - '0' < 10) ? c2 - '0' : c2 - 'A' + 10;

            holder[holderOffset++] = (byte) ((c1 << 4) + c2);
        }

        return holder;
    }

    public static String encodeCuboidPath(Cuboid cuboid) {
        String r = null;
        for (String dim : cuboid.getCuboidPath())
            r = r == null ? dim : r + "/" + dim;
        return r;
    }

    public static List<String> decodeCuboidPath(String path) {
        return Arrays.asList(path.split("/"));
    }

    public static byte[] fillCompositeKeyWithDec(int src, int decimalLen, byte[] holder, int holderOffset) {
        for (int i = holderOffset + decimalLen - 1; i >= holderOffset; i--)
            holder[i] = (byte) ('0' + (src - (src /= 10) * 10));
        return holder;
    }

    public static int readCompositeKeyDec(byte[] composite, int offset, int decimalLen) {
        int result = 0;
        for (int i = 0; i < decimalLen; i++)
            result = 10 * result + (composite[offset++] - '0');
        return result;
    }

    // ///////////////////////////////////////////////////////////////////////////////
    // all the Var*** stuff is a shameless rip-off of the RLE + zigzag
    // encoding used in protobuf as well.
    // ///////////////////////////////////////////////////////////////////////////////

    public static void writeVarUint32(DataOutput out, int val) throws IOException {
        for (;; val >>>= 7) {
            if ((val & ~0x7f) == 0) {
                out.writeByte((byte) val);
                break;
            } else
                out.writeByte((byte) (0x80 | val));
        }
    }

    public static void writeVarUint64(DataOutput out, long val) throws IOException {
        for (;; val >>>= 7) {
            if ((val & ~0x7f) == 0) {
                out.writeByte((byte) val);
                break;
            } else
                out.writeByte((byte) (0x80 | val));
        }
    }

    public static void writeVarInt32(DataOutput out, int val) throws IOException {
        writeVarUint32(out, (val << 1) ^ (val >> -1));
    }

    public static void writeVarInt64(DataOutput out, long val) throws IOException {
        writeVarUint64(out, (val << 1) ^ (val >> -1));
    }

    public static int readVarUint32(DataInput in) throws IOException {
        int accum = 0, bitsRead = 0;
        do {
            int c = in.readByte();
            if (c == -1)
                throw new IOException("Unexpected EOF while reading uint32");

            c &= 0xff;

            if ((c & 0x80) == 0)
                return accum | c << bitsRead;
            else
                accum |= (c & 0x7f) << bitsRead;
            bitsRead += 7;
        } while (bitsRead < 35);
        throw new IOException("Illegal Varint format");
    }

    public static int readVarInt32(DataInput in) throws IOException {
        int n = readVarUint32(in);
        return (n >>> 1) ^ -(n & 1);
    }

    public static long readVarUint64(DataInput in) throws IOException {
        long accum = 0;
        int bitsRead = 0;
        do {
            int c = in.readByte();
            if (c == -1)
                throw new IOException("Unexpected EOF while reading uint64");

            c &= 0xff;

            if ((c & 0x80) == 0)
                return accum | c << bitsRead;
            else
                accum |= (c & 0x7f) << bitsRead;
            bitsRead += 7;
        } while (bitsRead < 70);
        throw new IOException("Illegal Varint format");
    }

    public static long readVarInt64(DataInput in) throws IOException {
        long n = readVarUint64(in);
        return (n >>> 1) ^ -(n & 1);

    }

    /**
     * variable length encoding from protobuf. Would pack small positives (like
     * incremental ids or offsets) more efficiently whereas random bitsets would
     * take more space on average.
     * 
     * Same as the encoding used for uint32 values in protobuf.
     * 
     * @param bb
     * @param val
     */
    public static void setVarUint32(ByteBuffer bb, int val) throws IOException {
        for (;; val >>>= 7) {
            if ((val & ~0x7f) == 0) {
                bb.put((byte) val);
                break;
            } else
                bb.put((byte) (0x80 | val));
        }
    }

    public static int getVarUint32(ByteBuffer bb) throws IOException {
        int accum = 0, bitsRead = 0;
        do {
            int c = bb.get() & 0xff;

            if ((c & 0x80) == 0)
                return accum | c << bitsRead;
            else
                accum |= (c & 0x7f) << bitsRead;
            bitsRead += 7;
        } while (bitsRead < 35);
        throw new IOException("Illegal Varint format");
    }

    /**
     * this is probably not a very often case (better for encoding small
     * absolute values of both signs)
     * 
     * @param bb
     * @param val
     * @throws IOException
     */
    static void setVarInt32(ByteBuffer bb, int val) throws IOException {
        setVarUint32(bb, (val << 1) ^ (val >> 31));
    }

    static int getVarInt32(ByteBuffer bb) throws IOException {
        int n = getVarUint32(bb);
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * increment key by 1, catch situations with overflow. Hopefully, since it
     * is a little bit more purposed, it would be a little more efficent than
     * {@link Bytes#incrementBytes(byte[], long)}.
     * <P>
     * 
     * @param key
     * @param offset
     * @param length
     * @return true if increment resulted in carry-over bit overflow (such as
     *         FF->00), so no more keys.
     */
    public static boolean incrementKey(byte[] key, int offset, int length) {
        if (length == 0)
            return true;

        int i;
        for (i = offset + length - 1; i >= offset; i--) {
            key[i] = (byte) (1 + key[i]);
            if (key[i] != 0)
                break;
        }
        return !(i >= offset);
    }

    public static boolean decrementKey(byte[] key, int offset, int length) {
        if (length == 0)
            return true;

        int i;
        for (i = offset + length - 1; i >= offset; i--) {
            key[i] = (byte) (key[i] - 1);
            if (key[i] != 0xff)
                break;
        }
        return !(i >= offset);
    }

}
