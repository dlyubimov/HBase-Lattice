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
package com.inadco.hbl.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.util.IOUtil;

/**
 * Hbl Admin utility
 * 
 * @author dmitriy
 * 
 */
@Component("HblAdmin")
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class HblAdmin {

    private static final Logger s_log                        = Logger.getLogger(HblAdmin.class);

    public static final String  HBL_METRIC_FAMILY_STR        = "hbl";
    public static final byte[]  HBL_METRIC_FAMILY            = Bytes.toBytes(HBL_METRIC_FAMILY_STR);

    public static final String  HBL_DEFAULT_SYSTEM_TABLE_STR = "HBL_SYSTEM";
    public static final byte[]  HBL_DEFAULT_SYSTEM_TABLE     = Bytes.toBytes(HBL_DEFAULT_SYSTEM_TABLE_STR);

    public static final String  HBL_SYSTEM_FAMILY_STR        = "hbl_system";
    public static final byte[]  HBL_SYSTEM_FAMILY            = Bytes.toBytes(HBL_SYSTEM_FAMILY_STR);

    public static final String  HBL_MODEL_KEY_STR            = "MODEL";
    public static final byte[]  HBL_MODEL_KEY                = Bytes.toBytes(HBL_MODEL_KEY_STR);

    private byte[]              systemTable                  = HBL_DEFAULT_SYSTEM_TABLE;
    private Resource            cubeModel;
    private String              cubeModelYamlStr;
    private Cube                cube;

    /**
     * non-spring constructor Setup cube model by name of the model saved in HBL
     * system table.
     * 
     * @param modelName
     */
    public HblAdmin(String modelName, Configuration conf) throws IOException {
        this.cubeModel=readModelFromHBase(conf, modelName,systemTable);
        init();

    }

    /**
     * Non-spring constructor
     * 
     * @param cubeModel
     */
    public HblAdmin(Resource cubeModel) throws IOException {
        this.cubeModel = cubeModel;
        init();
    }

    /**
     * Spring constructor: look at what is @Required.
     */
    public HblAdmin() {
        super();
        // TODO Auto-generated constructor stub
    }

    public Resource getCubeModel() {
        return cubeModel;
    }

    public void setCubeModel(Resource cubeModel) {
        this.cubeModel = cubeModel;
    }

    public String getCubeModelYamlStr() {
        return cubeModelYamlStr;
    }

    public Cube getCube() {
        return cube;
    }

    @PostConstruct
    public void init() throws IOException {
        Validate.notNull(cubeModel);
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

    public void dropCube(Configuration conf) throws IOException {
        HBaseAdmin hba = new HBaseAdmin(conf);
        IOException lastErr = null;

        for (Cuboid c : cube.getCuboids()) {
            try {
                dropCuboid(hba, c);
            } catch (IOException exc) {
                s_log.error(exc.getMessage(), exc);
                lastErr = exc;
            }
        }
        if (lastErr != null)
            throw lastErr;
    }

    /**
     * deploys the cube model to hbase by creating necessary tables. if table
     * already exists, it doesn't currently change its attributes.
     * 
     */
    public void deployCube(Configuration conf) throws IOException {
        HBaseAdmin hba = new HBaseAdmin(conf);
        saveModel(hba, conf);
        for (Cuboid c : cube.getCuboids())
            initCuboid(hba, c);
    }

    public void saveModel(Configuration conf) throws IOException {
        HBaseAdmin hba = new HBaseAdmin(conf);
        saveModel(hba, conf);
    }

    private void initCuboid(HBaseAdmin admin, Cuboid c) throws IOException {

        byte[] tablename = Bytes.toBytes(c.getCuboidTableName());

        if (admin.tableExists(tablename)) {
            s_log.info(String.format("cuboid table %s already exists, cowardly refusing to re-create...",
                                     c.getCuboidTableName()));
            return;
        }

        HTableDescriptor htd = new HTableDescriptor(tablename);

        HColumnDescriptor hcd = new HColumnDescriptor(HBL_METRIC_FAMILY);
        hcd.setMaxVersions(c.getHbaseMaxVersions());
        hcd.setInMemory(c.isHbaseInMemory());
        hcd.setTimeToLive(c.getHbaseTTL());

        htd.addFamily(hcd);

        admin.createTable(htd);
    }

    private void saveModel(HBaseAdmin admin, Configuration conf) throws IOException {
        if (!admin.tableExists(systemTable)) {
            HTableDescriptor htd = new HTableDescriptor(systemTable);
            HColumnDescriptor hcd = new HColumnDescriptor(HBL_SYSTEM_FAMILY);
            /*
             * put in reasonable defaults, although admins can adjust it all
             * manually later if needed
             */
            hcd.setMaxVersions(10);
            hcd.setInMemory(true); // this is really tiny one
            hcd.setTimeToLive(Integer.MAX_VALUE); // indefinite
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
        HTable stable = new HTable(systemTable);
        Put put =
            new Put(HBL_MODEL_KEY).add(HBL_SYSTEM_FAMILY,
                                       Bytes.toBytes(cube.getName()),
                                       Bytes.toBytes(cubeModelYamlStr));
        stable.put(put);
    }

    private void dropCuboid(HBaseAdmin admin, Cuboid c) throws IOException {
        byte[] tablename = Bytes.toBytes(c.getCuboidTableName());

        if (admin.tableExists(tablename)) {
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
    }

    public static Resource readModelFromHBase(Configuration conf, String modelName, byte[] systemTable) throws IOException {
        Validate.notNull(modelName);

        HTable htable = new HTable(conf, systemTable);
        Result r = htable.get(new Get(HBL_MODEL_KEY).addColumn(HBL_SYSTEM_FAMILY, Bytes.toBytes(modelName)));
        byte[] rbytes;
        if (r == null || (rbytes = r.getValue(HBL_SYSTEM_FAMILY, Bytes.toBytes(modelName))) == null)
            throw new IOException(String.format("Model '%s' was not found in the system table.", modelName));
        return new ByteArrayResource(rbytes);
    }
}
