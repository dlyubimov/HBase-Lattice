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

import com.inadco.hbl.client.HierarchyMember;
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
    public void getKey(Object member, byte[] buff, int offset) {
        if (member instanceof HierarchyMember) {
            HierarchyMember hm = (HierarchyMember) member;
            getKey(hm.getMember(), hm.getDepth(), buff, offset);
        } else {
            getKey(member, 2, buff, offset);
        }
    }

    @Override
    public void getKey(Object member, int hierarchyDepth, byte[] buff, int offset) {
        switch (hierarchyDepth) {
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
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.MONTH) + 1, 2, buff, offset);
        offset += 2;
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.DATE), 2, buff, offset);
        offset += 2;
        HblUtil.fillCompositeKeyWithDec(gcal.get(Calendar.HOUR_OF_DAY), 2, buff, offset);
    }

    private static GregorianCalendar toGCal(Object member) {
        GregorianCalendar gcal = new GregorianCalendar();
        gcal.setTimeInMillis((Long) member);
        // flush
        gcal.getTimeInMillis();
        gcal.setTimeZone(UTC);
        return gcal;

    }

}
