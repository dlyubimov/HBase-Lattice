package com.inadco.hbl.test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.util.HblUtil;
import com.inadco.hbl.util.IOUtil;
import com.sun.jersey.api.core.ClasspathResourceConfig;

/**
 * this really requires local hbase up and running..
 * 
 * @author dmitriy
 * 
 */
public class CompositeFilterTest {

    private Deque<Closeable>    closeables = new ArrayDeque<Closeable>();
    // private static final byte[]
    // TEST_TABLENAME=Bytes.toBytes("COMPOSITE_FILTER_TEST");
    // private static final byte[] TEST_FAMILYNAME=Bytes.toBytes("TEST_COL");
    private final Configuration conf       = new Configuration();
    private HblAdmin            hblAdmin;

    private long[][]            testData   = new long[][] { new long[] { 1l, 1l, 1l }, new long[] { 1l, 2l, 2l },
        new long[] { 2l, 1l, 3l }, new long[] { 2l, 2l, 4l } };

    @BeforeClass
    public void init() throws IOException {

        hblAdmin = new HblAdmin(new ClassPathResource("testModel1.yaml"));

        dropTable();
        createTable();
    }

    private void insertTestData() throws IOException {
        Cuboid testCuboid = hblAdmin.getCube().findCuboidForPath(HblUtil.decodeCuboidPath("dim1/dim2"));
        Assert.notNull(testCuboid);

        byte[] key = new byte[testCuboid.getKeyLen()];
        for (long[] data : testData) {
            Dimension dim1 = testCuboid.getCuboidDimensions().get(0);
            Dimension dim2 = testCuboid.getCuboidDimensions().get(1);
        }
    }

    private void createTable() throws IOException {

        hblAdmin.deployCube(conf);

    }

    private void dropTable() throws IOException {
        hblAdmin.dropCube(conf);
    }

    @AfterClass
    public void close() throws IOException {
        try {
            dropTable();
        } finally {
            IOUtil.closeAll(closeables);
        }
    }
    
    public void testFilter () throws IOException { 
        
    }

}
