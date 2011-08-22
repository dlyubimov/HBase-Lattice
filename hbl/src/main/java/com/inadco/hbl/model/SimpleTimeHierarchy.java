package com.inadco.hbl.model;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.util.HblUtil;

/**
 * standard time hierarchy supporting [ALL].[year-month].[date-hour] buckets
 * 
 * @author dmitriy
 * 
 */
public class SimpleTimeHierarchy extends AbstractHierarchy {

    // so we organize key as [YYYYMM][DDHH] concatenated literals.
    // this, length=6+4=10

    private static final int      YM_KEYLEN = 6;
    private static final int      DH_KEYLEN = 4;
    private static final int      KEYLEN    = YM_KEYLEN + DH_KEYLEN;
    private static final TimeZone UTC       = TimeZone.getTimeZone("UTC");

    public SimpleTimeHierarchy(String name) {
        super(name, new String[] { "ALL", "year-month", "date-hour" });
    }

    @Override
    public int getKeyLen() {
        return KEYLEN;
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
