package com.inadco.hbl.math.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.inadco.hbl.math.aggregators.IrregularSamplingSummarizer;
import com.inadco.hbl.math.aggregators.OnlineCannyAvgSummarizer;
import com.inadco.hbl.math.aggregators.OnlineCannyRateSummarizer;
import com.inadco.hbl.math.aggregators.OnlineExpAvgSummarizer;
import com.inadco.hbl.math.aggregators.OnlineExpBiasedBinomialSummarizer;
import com.inadco.hbl.math.aggregators.OnlineExpRateSummarizer;

/**
 * Helper to parse/bootstrap stuff
 * 
 * @author dmitriy
 * 
 */

public final class PigSummarizerHelper {

    private PigSummarizerHelper() {
    }

    /**
     * Parse init string passed to summarizer functions.
     * <P>
     * 
     * The init string is a name=value pairs, space/tab separated.
     * 
     * @param str
     *            init string
     * @return properties parsed out as sting values only.
     */
    public static Properties parseInitString(String str) {

        Properties result = new Properties();
        for (String nvstr : str.split("[ |\\t]+")) {
            String[] nv = nvstr.split("=");
            Validate.isTrue(nv.length == 2, "configuration properties should be name=value pairs");
            Validate.isTrue(!result.contains(nv[0]), "duplicate parameter passed in");
            result.put(nv[0], nv[1]);
        }

        return result;

    }

    public static IrregularSamplingSummarizer createSummarizer(String initString, boolean bareInstance) {

        Properties props = parseInitString(initString);
        String type = props.getProperty("type");
        Validate.notNull(type, "type parameter is required");
        type = type.toUpperCase();

        SupportedIrregularsEnum typeEnum;
        try {
            typeEnum = SupportedIrregularsEnum.valueOf(type);
        } catch (IllegalArgumentException exc) {
            throw new IllegalArgumentException(String.format("Summarizer type \"%s\" is not supported.", type));
        }

        switch (typeEnum) {
        case EXP_AVG:
            return createExpAvgSum(props, bareInstance);
        case EXP_BIASED_BINOMIAL:
            return createExpBinomialSum(props, bareInstance);
        case EXP_RATE:
            return createExpRateSum(props, bareInstance);
        case CANNY_AVG:
            return createCannyAvgSum(props, bareInstance);
        case CANNY_RATE:
            return createCannyRateSum(props, bareInstance);

        }
        return null;
    }

    public static enum SupportedIrregularsEnum {
        EXP_AVG, EXP_BIASED_BINOMIAL, EXP_RATE, CANNY_AVG, CANNY_RATE
    }

    static OnlineExpAvgSummarizer createExpAvgSum(Properties props, boolean bareInstance) {

        if (bareInstance)
            return new OnlineExpAvgSummarizer();

        double dt;
        double m = OnlineExpAvgSummarizer.DEFAULT_HISTORY_MARGIN;

        if (props.containsKey("m"))
            m = Double.parseDouble(props.getProperty("m"));

        Validate.isTrue(props.containsKey("dt"), "dt parameter is required for exp avg summarizer");
        dt = Double.parseDouble(props.getProperty("dt"));

        return new OnlineExpAvgSummarizer(dt, m);

    }

    static OnlineCannyAvgSummarizer createCannyAvgSum(Properties props, boolean bareInstance) {
        if (bareInstance)
            return new OnlineCannyAvgSummarizer();

        double dt;
        double m = OnlineCannyAvgSummarizer.DEFAULT_MARGIN;
        double k = OnlineCannyAvgSummarizer.DEFAULT_K;

        if (props.containsKey("m"))
            m = Double.parseDouble(props.getProperty("m"));
        if (props.containsKey("k"))
            k = Double.parseDouble(props.getProperty("k"));

        Validate.isTrue(props.containsKey("dt"), "dt parameter is required for exp avg summarizer");
        dt = Double.parseDouble(props.getProperty("dt"));

        return new OnlineCannyAvgSummarizer(dt, m, k);

    }

    static OnlineCannyRateSummarizer createCannyRateSum(Properties props, boolean bareInstance) {
        double dt;
        double m = OnlineCannyAvgSummarizer.DEFAULT_MARGIN;
        double k = OnlineCannyAvgSummarizer.DEFAULT_K;

        if (props.containsKey("m"))
            m = Double.parseDouble(props.getProperty("m"));
        if (props.containsKey("k"))
            k = Double.parseDouble(props.getProperty("k"));

        Validate.isTrue(props.containsKey("dt"), "dt parameter is required for exp avg summarizer");
        dt = Double.parseDouble(props.getProperty("dt"));

        return new OnlineCannyRateSummarizer(dt, m, k);

    }

    static OnlineExpBiasedBinomialSummarizer createExpBinomialSum(Properties props,
                                                                                   boolean bareInstance) {

        if (bareInstance)
            return new OnlineExpBiasedBinomialSummarizer();

        double dt;
        double m = OnlineExpBiasedBinomialSummarizer.DEFAULT_HISTORY_MARGIN;
        double p0;
        double epsilon = OnlineExpBiasedBinomialSummarizer.DEFAULT_EPSILON;

        if (props.containsKey("m"))
            m = Double.parseDouble(props.getProperty("m"));
        if (props.containsKey("epsilon"))
            epsilon = Double.parseDouble(props.getProperty("epsilon"));

        Validate.isTrue(props.containsKey("dt"), "dt parameter is required for exp binomial summarizer");
        dt = Double.parseDouble(props.getProperty("dt"));

        Validate.isTrue(props.containsKey("p0"), "p0 parameter is required for exp binomial summarizer");
        p0 = Double.parseDouble(props.getProperty("p0"));

        return new OnlineExpBiasedBinomialSummarizer(p0, epsilon, dt, m);
    }

    static OnlineExpRateSummarizer createExpRateSum(Properties props, boolean bareInstance) {

        if (bareInstance)
            return new OnlineExpRateSummarizer();

        double dt;
        double m = OnlineExpRateSummarizer.DEFAULT_HISTORY_MARGIN;

        if (props.containsKey("m"))
            m = Double.parseDouble(props.getProperty("m"));

        Validate.isTrue(props.containsKey("dt"), "dt parameter is required for exp rate summarizer");
        dt = Double.parseDouble(props.getProperty("dt"));

        return new OnlineExpRateSummarizer(dt, m);
    }

    static <T extends Writable> DataOutputBuffer ser2bytes(T summarizer, DataOutputBuffer dob) throws IOException {
        if (summarizer == null)
            return null;
        dob.reset();
        summarizer.write(dob);
        dob.close();
        return dob;
    }

    /**
     * 
     * @param holderInstance
     * @param dib
     *            input
     * @return
     * @throws IOException
     */
    static IrregularSamplingSummarizer bytes2ser(IrregularSamplingSummarizer holderInstance, DataInputBuffer dib)
        throws IOException {
        if (dib == null)
            return null;
        holderInstance.readFields(dib);
        return holderInstance;
    }

    /**
     * combines bunch of serialized summarizer states in bag parameter onto
     * existing summarizer (if any)
     * 
     * @param bag
     * @param dib
     *            temporary buffer used in deserializing bagged summarizers
     * @param summarizerInitStr
     *            init string for new summarizers
     * @param summarizer
     *            pre-existing sum, if any
     * @param buffer
     *            temp buffer to deserialize bagged summarizers
     * @return combined summarizer (summarizer or new summarizer if
     *         summarizer==null)
     * @throws IOException
     */
    static IrregularSamplingSummarizer combine(Tuple bag,
                                               DataInputBuffer dib,
                                               String summarizerInitStr,
                                               IrregularSamplingSummarizer summarizer,
                                               IrregularSamplingSummarizer buffer) throws IOException {

        DataBag db = DataType.toBag(bag.get(0));
        for (Tuple t : db) {
            byte[] dba = DataType.toBytes(t.get(0));
            dib.reset(dba, dba.length);
            IrregularSamplingSummarizer sum = PigSummarizerHelper.bytes2ser(buffer, dib);
            if (summarizer == null) {
                summarizer = PigSummarizerHelper.createSummarizer(summarizerInitStr, true);
                summarizer.assign(sum);
            } else
                summarizer.combine(sum);
        }
        return summarizer;

    }

}
