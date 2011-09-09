package com.inadco.hbl.test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.commons.lang.Validate;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.util.IOUtil;

public class CompositeFilterTest {
    
    private Deque<Closeable> closeables = new ArrayDeque<Closeable>();
    
    @BeforeClass
    public void init() throws IOException { 
        
        InputStream is = CompositeFilterTest.class.getClassLoader().getResourceAsStream("testModel1.yaml");
        Validate.notNull(is);
        
        closeables.addFirst(is);
        
        
        String yamlModelStr=IOUtil.fromStream(is, "utf-8");
        IOUtil.closeAll(closeables);
        Cube cube = YamlModelParser.parseYamlModel(yamlModelStr);
        
    }
    
    @AfterClass
    public void close() throws IOException { 
        IOUtil.closeAll(closeables);
    }

}
