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
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.HierarchyMember;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.HblException;
import com.inadco.hbl.client.impl.Slice;
import com.inadco.hbl.util.HblUtil;

/**
 * standard time hierarchy supporting [ALL].[year-month].[date-hour] buckets.
 * <P>
 * 
 * I.e. the lowest granularity of a member bucket is hour.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class SimpleTimeHourHierarchy extends AbstractHierarchy {

    // so we organize key as [YYYYMM][DDHH] concatenated literals.
    // this, length=6+4=10

    private static final int      YM_KEYLEN = 6;
    private static final int      DH_KEYLEN = 4;
    private static final int      KEYLEN    = YM_KEYLEN + DH_KEYLEN;
    private static final TimeZone UTC       = TimeZone.getTimeZone("UTC");

    public SimpleTimeHourHierarchy(String name) {
        super(name, new String[] { "ALL", "year-month", "date-hour" });
    }

    @Override
    public int getKeyLen() {
        return KEYLEN;
    }

    @Override
    public int getSubkeyLen(int level) {
        switch (level) {
        case 0:
            return 0;
        case 1:
            return YM_KEYLEN;
        case 2:
            return KEYLEN;
        default:
            Validate.isTrue(false, "invalid depth");
            return -1; // not reached
        }

    }

    @Override
    public void getKey(Object member, byte[] buff, int offset) {
        if (member instanceof HierarchyMember) {
            HierarchyMember hm = (HierarchyMember) member;
            getKey(hm.getMember(), hm.getDepth(), buff, offset);
        } else {
            getKey(member, 2, buff, offset);
        }
    }

    @Override
    public void getKey(Object member, int level, byte[] buff, int offset) {
        switch (level) {
        case 0:
            getAllKey(buff, offset);
            break;
        case 1:
            GregorianCalendar gcal = toGCal(member);
            getMonthlyKey(buff, offset, gcal);
            break;
        case 2:
            gcal = toGCal(member);
            getHourlyKey(buff, offset, gcal);
            break;
        default:
            Validate.isTrue(false, "invalid hierarchy depth");
        }
    }

    @Override
    public int getDepth() {
        return 3;
    }

    public void getAllKey(byte[] buff, int offset) {
        Arrays.fill(buff, offset, offset + KEYLEN, (byte) 0);
    }

    @Override
    public int keyDepth(byte[] buff, int offset) {
        if (buff[offset] == 0)
            return 0; // all-key
        if (buff[offset + 6] == 0)
            return 1; // monthly key
        return 2; // hourly key otherwise.
    }

    public void getMonthlyKey(byte[] buff, int offset, GregorianCalendar gcal) {
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.YEAR), 4, buff, offset);
        offset += 4;
        /* month field is 0-11 in GC */
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.MONTH) + 1, 2, buff, offset);
        offset += 2;
        Arrays.fill(buff, offset, offset + DH_KEYLEN, (byte) 0);
    }

    public void getHourlyKey(byte[] buff, int offset, GregorianCalendar gcal) {
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.YEAR), 4, buff, offset);
        offset += 4;
        /* month field is 0-11 in GC */
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.MONTH) - Calendar.JANUARY + 1, 2, buff, offset);
        offset += 2;
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.DATE), 2, buff, offset);
        offset += 2;
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.HOUR_OF_DAY), 2, buff, offset);
    }

    @Override
    public Object getMember(byte[] buff, int offset) throws HblException {
        int depth = keyDepth(buff, offset);
        int year = 0;
        int month = 0;
        int date = 0;
        int hour = 0;

        switch (depth) {
        case 0:
            throw new HblException(
                "Unable to make a presentation of an [ALL] hierarchy member in terms of concrete calendar.");
        case 2:
            date = HblUtil.readCompositeKeyDec(buff, offset + 6, 2);
            hour = HblUtil.readCompositeKeyDec(buff, offset + 8, 2);
        case 1:
            year = HblUtil.readCompositeKeyDec(buff, offset, 4);
            month = HblUtil.readCompositeKeyDec(buff, offset + 4, 2) - Calendar.JANUARY + 1;
            break;
        default:
            throw new HblException("unexpected hierarchy depth in the key.");
        }
        // we return stuff as calendar objects here.
        GregorianCalendar gc = new GregorianCalendar(UTC);
        gc.set(Calendar.YEAR, year);
        gc.set(Calendar.MONTH, month);
        gc.set(Calendar.DATE, date);
        gc.set(Calendar.HOUR_OF_DAY, hour);
        gc.set(Calendar.MINUTE, 0);
        gc.set(Calendar.SECOND, 0);
        gc.set(Calendar.MILLISECOND, 0);
        return gc;

    }

    private static GregorianCalendar toGCal(Object member) {
        if (member instanceof GregorianCalendar) {
            GregorianCalendar gcal = new GregorianCalendar(UTC);
            gcal.setTimeInMillis(((GregorianCalendar) member).getTimeInMillis());
            return gcal;
        } else {
            // assume epoch milliseconds
            GregorianCalendar gcal = new GregorianCalendar(UTC);
            gcal.setTimeInMillis((Long) member);
            return gcal;
        }

    }

    @Override
    public Range[] optimizeSliceScan(Slice slice, boolean allowComplements) {
        // todo: optimize this better using complement scans.
        Range[] ranges = super.optimizeSliceScan(slice, allowComplements);
        ranges[0].setSubkeyLen(KEYLEN);
        return ranges;
    }

}
