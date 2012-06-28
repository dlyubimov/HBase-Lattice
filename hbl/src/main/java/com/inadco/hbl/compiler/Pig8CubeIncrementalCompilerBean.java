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
package com.inadco.hbl.compiler;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Measure;
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.piggybank.AggregationFromMeasureBag;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.util.HblUtil;
import com.inadco.hbl.util.IOUtil;

/**
 * Pig8-based cube incremental compiler. Guarantees to commit hbase in
 * idempotent fashion if seen input only once (i.e. incrementally).
 * <P>
 * 
 * Warning: if run periodically, doesn't protect against nasty effects such as
 * pig memory leaks or pig non-reentrancy or job backlog overrun. It's now
 * assumed invoker's /caller's responsibility to do so .
 * <P>
 * 
 * @author dmitriy
 * 
 */
@Component("Pig8CubeIncrementalCompiler")
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class Pig8CubeIncrementalCompilerBean {

    public static final String PROP_CUBEMODEL                  = "com.inadco.hbl.cubemodel";

    /*
     * regex, not verbatim
     */
    public static final String SUBS_OPEN                       = "$hbl:{";
    public static final String SUBS_CLOSE                      = "}";

    public static final String DEFAULT_PIG_INPUT_RELATION_NAME = "HBL_INPUT";

    protected Resource         cubeModel;
    protected Resource         pigPreambula;
    protected String           inputRelationName;
    protected int              parallel                        = 2;
    protected List<Cuboid>     compilationCuboids;

    protected Cube             cube;
    protected String           cubeModelYamlStr;
    protected Set<String>      measureInclude;
    protected Set<String>      measureExclude;
    protected Set<String>      cuboidGroupsInclude;

    /**
     * non-spring version
     * 
     * @param cubeModel
     *            cube model specification
     * 
     * @param pigPreambula
     *            pig script fragment that establishes the input
     * @throws IOException
     */
    public Pig8CubeIncrementalCompilerBean(Resource cubeModel, Resource pigPreambula, int parallel) throws IOException {
        this(cubeModel, pigPreambula, parallel, DEFAULT_PIG_INPUT_RELATION_NAME);
    }

    /**
     * A version of constructor that takes cube name and loads model from the
     * hbase system table.
     * 
     * @param conf
     *            hbase configuration
     * @param cubeName
     *            cube model name
     * @param pigPreambula
     *            the resource defining pig preambula (Pig fragment defining
     *            compilation input)
     * @param parallel
     *            number of reducers for parallelizable Pig steps
     * @throws IOException
     *             when I/O condition occurs.
     */
    public Pig8CubeIncrementalCompilerBean(Configuration conf, String cubeName, Resource pigPreambula, int parallel)
        throws IOException {
        this(conf, cubeName, pigPreambula, parallel, DEFAULT_PIG_INPUT_RELATION_NAME);
    }

    public Pig8CubeIncrementalCompilerBean(Configuration conf,
                                           String cubeName,
                                           Resource pigPreambula,
                                           int parallel,
                                           String inputRelationName) throws IOException {
        this.cubeModel = HblAdmin.readModelFromHBase(conf, cubeName, HblAdmin.HBL_DEFAULT_SYSTEM_TABLE);
        this.pigPreambula = pigPreambula;
        this.inputRelationName = inputRelationName;
        this.parallel = parallel;
        init();
    }

    /**
     * non-spring version
     * 
     * @param cubeModel
     *            cube model specification
     * @param pigPreambula
     *            pig script fragment that retrieves the input
     * @param relation
     *            name to use for input out of pigPreambula fragment
     */
    public Pig8CubeIncrementalCompilerBean(Resource cubeModel,
                                           Resource pigPreambula,
                                           int parallel,
                                           String inputRelationName) throws IOException {
        super();
        this.cubeModel = cubeModel;
        this.pigPreambula = pigPreambula;
        this.inputRelationName = inputRelationName;
        init();
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    /**
     * Spring constructor
     */
    public Pig8CubeIncrementalCompilerBean() {
        super();
        // TODO Auto-generated constructor stub
    }

    public Resource getCubeModel() {
        return cubeModel;
    }

    @Required
    public void setCubeModel(Resource cubeModel) {
        this.cubeModel = cubeModel;
    }

    @Required
    public Resource getPigPreambula() {
        return pigPreambula;
    }

    public void setPigPreambula(Resource preambula) {
        this.pigPreambula = preambula;
    }

    public String getInputRelationName() {
        return inputRelationName;
    }

    @Required
    public void setInputRelationName(String inputRelationName) {
        this.inputRelationName = inputRelationName;
    }

    public Set<String> getMeasureInclude() {
        return measureInclude;
    }

    /**
     * measures to include. If this property is set, the script created will
     * only expect those measures and assume all other measures at 0.
     * 
     * @param measureInclude
     */
    public void setMeasureInclude(Set<String> measureInclude) {
        this.measureInclude = measureInclude;
    }

    public Set<String> getMeasureExclude() {
        return measureExclude;
    }

    public void setMeasureExclude(Set<String> measureExclude) {
        this.measureExclude = measureExclude;
    }

    public Set<String> getCuboidGroupsInclude() {
        return cuboidGroupsInclude;
    }

    /**
     * Set the cuboid groups to include in this compilation run only. NULL or
     * empty set means include all cuboids.
     * 
     * @param cuboidGroupsInclude
     *            cuboids group to include
     */
    public void setCuboidGroupsInclude(Set<String> cuboidGroupsInclude) {
        this.cuboidGroupsInclude = cuboidGroupsInclude;
    }

    @PostConstruct
    public void init() throws IOException {
        Validate.notNull(cubeModel);
        Validate.notNull(pigPreambula);
        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            InputStream cubeIs = cubeModel.getInputStream();
            closeables.addFirst(cubeIs);
            cubeModelYamlStr = IOUtil.fromStream(cubeIs, "utf-8");
            cube = YamlModelParser.parseYamlModel(cubeModelYamlStr);

        } finally {
            IOUtil.closeAll(closeables);
        }

    }

    public String preparePigSource(String workDir) throws IOException {

        Deque<Closeable> closeables = new ArrayDeque<Closeable>();
        try {
            Map<String, String> substitutes = new HashMap<String, String>();

            InputStream is =
                Pig8CubeIncrementalCompilerBean.class.getClassLoader().getResourceAsStream("hbl-compiler.pig");
            Validate.notNull(is, "hbl-compiler.pig not found");
            closeables.addFirst(is);
            String compilerSrc = IOUtil.fromStream(is, "utf-8");

            substitutes.put("workDir", workDir);
            substitutes.put("inputRelation", inputRelationName);
            substitutes.put("parallel", "" + parallel);
            substitutes.put("cubeName", cube.getName());

            /*
             * generate list of cuboids included in this compilation run
             */

            compilationCuboids = new LinkedList<Cuboid>();
            if (cuboidGroupsInclude == null || cuboidGroupsInclude.size() == 0)
                compilationCuboids.addAll(cube.getCuboids());
            else
                for (Cuboid cuboid : cube.getCuboids()) {
                    String group = cuboid.getCompilerGroup();
                    if (group != null && cuboidGroupsInclude.contains(group))
                        compilationCuboids.add(cuboid);
                }

            Validate.isTrue(compilationCuboids.size() > 0,
                            "no cuboids found to compile (perhaps due to compilation group filtering?)");

            // preambula
            generatePreambula(substitutes, closeables);

            // measures
            generateCommonDefs(substitutes, closeables);

            // $hbl:{cuboidStoreDefs}
            generateCuboidStoreDefs(substitutes, closeables);

            // cuboid bodies
            generateBody(substitutes, closeables);

            // substitute all at once
            compilerSrc = preprocess(compilerSrc, substitutes);
            return compilerSrc;

        } finally {
            IOUtil.closeAll(closeables);
        }

    }

    private boolean measureNotIncluded(String measureName) {
        return (measureInclude != null && !measureInclude.contains(measureName))
            || (measureExclude != null && measureExclude.contains(measureName));

    }

    private void generatePreambula(Map<String, String> substitutes, Deque<Closeable> closeables) throws IOException {
        InputStream is = pigPreambula.getInputStream();
        Validate.notNull(is, "preambula not found");
        closeables.addFirst(is);
        String preambulaStr = IOUtil.fromStream(is, "utf-8");
        substitutes.put("preambula", preambulaStr);
    }

    private void generateCommonDefs(Map<String, String> substitutes, Deque<Closeable> closeables) throws IOException {
        Validate.notNull(cube);
        String cubeModel = YamlModelParser.encodeCubeModel(cubeModelYamlStr);
        substitutes.put("cubeModel", cubeModel);

        // String def =
        // String.format("DEFINE hbl_m2d com.inadco.hbl.piggybank.Measure2Double('%s');\n",
        // YamlModelParser.encodeCubeModel(cubeModelYamlStr));
        // substitutes.put("measure2DoubleDef", def);
        //
        // def =
        // String.format("DEFINE hbl_d2k com.inadco.hbl.piggybank.Dimensions2CuboidKey('%s');\n",
        // YamlModelParser.encodeCubeModel(cubeModelYamlStr));

        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, ? extends Measure> me : cube.getMeasures().entrySet()) {
            String measureName = me.getValue().getName();

            if (measureNotIncluded(measureName))
                continue;

            String def =
                String.format("DEFINE %s %s('%s','n','$cubeModel');\n",
                              getMeasureAggregateFuncName(me.getValue()),
                              AggregationFromMeasureBag.class.getName(),
                              measureName);
            sb.append(def);
            def =
                String.format("DEFINE %s %s('%s','y','$cubeModel');\n",
                              getMeasureCombineFuncName(me.getValue()),
                              AggregationFromMeasureBag.class.getName(),
                              measureName);
            sb.append(def);
        }
        substitutes.put("measureDefs", sb.toString());
    }

    private static String getMeasureAggregateFuncName(Measure m) {
        return String.format("hbl_aggr_%s", m.getName());
    }

    private static String getMeasureCombineFuncName(Measure m) {
        return String.format("hbl_comb_%s", m.getName());

    }

    private void generateCuboidStoreDefs(Map<String, String> substitutes, Deque<Closeable> closeables)
        throws IOException {

        StringBuffer sb = new StringBuffer();
        // for (Cuboid cuboid : cube.getCuboids())
        for (Cuboid cuboid : compilationCuboids)
            sb.append(generateCuboidStorageDef(cube, cuboid));
        substitutes.put("cuboidStoreDefs", sb.toString());

    }

    private String generateCuboidStorageDef(Cube cube, Cuboid cuboid) throws IOException {
        StringBuffer sb = new StringBuffer();
        StringBuffer hbaseSpecs = new StringBuffer();

        for (Measure m : cube.getMeasures().values()) {
            if (measureNotIncluded(m.getName()))
                continue;
            hbaseSpecs.append(generateHbaseByteArrayStorageSpec(m));
            hbaseSpecs.append(' ');
        }

        sb.append(String.format("DEFINE store_%s com.inadco.ecoadapters.pig.HBaseProtobufStorage ('%s');\n",
                                cuboid.getCuboidTableName(),
                                hbaseSpecs.toString()));

        sb.append(String.format("DEFINE get_%s com.inadco.ecoadapters.pig.HBaseGet('%1$s', '%s');\n",
                                cuboid.getCuboidTableName(),
                                hbaseSpecs.toString()));

        return sb.toString();
    }

    private void generateBody(Map<String, String> substitutes, Deque<Closeable> closeables) throws IOException {

        InputStream bodyTemplateIs =
            Pig8CubeIncrementalCompilerBean.class.getClassLoader().getResourceAsStream("hbl-body.pig");
        Validate.notNull(bodyTemplateIs, "body template resource not found");
        closeables.addFirst(bodyTemplateIs);

        String bodyTemplate = IOUtil.fromStream(bodyTemplateIs, "utf-8");

        Map<String, String> bodySubstitutes = new HashMap<String, String>();
        StringBuffer sbBody = new StringBuffer();
        // for (Cuboid c : cube.getCuboids()) {
        for (Cuboid c : compilationCuboids) {
            bodySubstitutes.clear();
            bodySubstitutes.putAll(substitutes);
            generateCuboidBody(bodySubstitutes, closeables, c);
            sbBody.append(preprocess(bodyTemplate, bodySubstitutes));
        }
        substitutes.put("body", sbBody.toString());
    }

    private void generateCuboidBody(Map<String, String> substitutes, Deque<Closeable> closeables, Cuboid cuboid)
        throws IOException {
        substitutes.put("cuboidPath", HblUtil.encodeCuboidPath(cuboid));
        substitutes.put("cuboidTable", cuboid.getCuboidTableName());

        // this kind of largely relies on the fact that iterating over measure
        // values comes in the same order.
        // todo: standardize the measure order in the codegen.

        // measure evaluation -- this is actually teh same for all cuboids at
        // this time.
        StringBuffer sb = new StringBuffer();
        for (Measure m : cube.getMeasures().values()) {

            if (measureNotIncluded(m.getName()))
                continue;

            if (sb.length() != 0)
                sb.append(", ");
            sb.append(m.getName());
        }
        substitutes.put("measuresEval", sb.toString());

        sb.setLength(0);
        for (Measure m : cube.getMeasures().values()) {

            if (measureNotIncluded(m.getName()))
                continue;

            if (sb.length() != 0)
                sb.append(",");
            sb.append("\n  ");
            sb.append(generateMeasureMetricEval(m, "GROUP_" + cuboid.getCuboidTableName()));
        }
        substitutes.put("measureMetricEvals", sb.toString());

        sb.setLength(0);
        for (Measure m : cube.getMeasures().values()) {

            if (measureNotIncluded(m.getName()))
                continue;

            if (sb.length() != 0)
                sb.append(",");
            sb.append("\n  ");
            sb.append(generateMeasureMetricMerge(m));
        }
        substitutes.put("measureMetricMerges", sb.toString());

        sb.setLength(0);
        for (Measure m : cube.getMeasures().values()) {

            if (measureNotIncluded(m.getName()))
                continue;

            if (sb.length() != 0)
                sb.append(",");
            sb.append("\n  ");
            sb.append(generateMeasureMetricSchema(m));
        }
        substitutes.put("measureMetricsSchema", sb.toString());

        // dim key evaluation -- dimensions in the order of cuboid path
        sb.setLength(0);
        for (String dim : cuboid.getCuboidPath()) {
            if (sb.length() != 0)
                sb.append(", ");
            sb.append(dim);
        }

        substitutes.put("cuboidKeyEval",
                        String.format("hbl_d2k('%s', %s)", HblUtil.encodeCuboidPath(cuboid), sb.toString()));

    }

    protected String generateHbaseProtoStorageSpec(Measure measure) {
        return String.format("%s:%s:%s",
                             Bytes.toString(HblAdmin.HBL_METRIC_FAMILY),
                             measure.getName(),
                             Aggregation.class.getName()
                                 .replaceAll(Pattern.quote("$"), Matcher.quoteReplacement("\\$")));
    }

    private String generateHbaseByteArrayStorageSpec(Measure measure) {
        return String.format("%s:%s", Bytes.toString(HblAdmin.HBL_METRIC_FAMILY), measure.getName());
    }

    private static String preprocess(String src, Map<String, String> substitutes) {

        /*
         * this actually is not quite correct way to do it (for one, we hope all
         * names match \w and if not, then they may be treated as regexes, not
         * as constants!). Second, since it is sequential replacements, values
         * will be matched against parameter string in subsequent replacements,
         * which is not only incorrect but also creates undetermined result.
         * This really needs a more neat work such as parameter preprocessor in
         * pig, but this two-liner is the easiest and fastest thing i can do
         * right now.
         */

        for (Map.Entry<String, String> entry : substitutes.entrySet())
            src =
                src.replaceAll(Pattern.quote(SUBS_OPEN + entry.getKey() + SUBS_CLOSE),
                               Matcher.quoteReplacement(entry.getValue()));
        return src;

    }

    private static String generateMeasureMetricEval(Measure m, String groupName) {
        // return
        // String.format("TOTUPLE(SUM(%1$s.%2$s),COUNT_STAR(%1$s)) as %3$s",
        // groupName,
        // metricName,
        // generateMeasureMetricSchema(metricName));
        return String.format("%s(%s.%s) as %3$s", getMeasureAggregateFuncName(m), groupName, m.getName());
    }

    private static String generateMeasureMetricMerge(Measure m) {
        // return String.format("TOTUPLE( " +
        // "(hbl_old.%1$s is null?%1$s.sum:hbl_old.%1$s.sum+%1$s.sum),"
        // + "(hbl_old.%1$s is null?%1$s.cnt:hbl_old.%1$s.cnt+%1$s.cnt)" +
        // ") as %2$s",
        // metricName,
        // generateMeasureMetricSchema(metricName));
        return String.format("(hbl_old.%1$s is null?%1$s:%2$s(TOBAG(hbl_old.%1$s,%1$s))) as %1$s",
                             m.getName(),
                             getMeasureCombineFuncName(m));
    }

    public static String generateMeasureMetricSchema(Measure m) {
        return String.format("%s:bytearray", m.getName());
    }

}
