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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.Validate;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.util.HblUtil;

/**
 * Simple Cuboid implementation
 * 
 * @author dmitriy
 * 
 */
public class SimpleCuboid implements Cuboid {

    protected String          tablePrefix;
    protected int             keyLen;

    protected List<String>    path                   = new LinkedList<String>();
    protected List<String>    unmodifiablePath       = Collections.unmodifiableList(path);

    protected List<Dimension> dimensions             = new ArrayList<Dimension>();
    protected List<Dimension> unmodifiableDimensions = Collections.unmodifiableList(dimensions);

    // hbase attributes as properties only

    protected int             hbaseTTL               = 3600 * 24 * 90;

    protected boolean         hbaseInMemory          = true;

    protected int             hbaseMaxVersions       = 1;

    protected Cube            parentCube;

    protected String          compilerGroup;

    public SimpleCuboid() {
        super();
    }

    public SimpleCuboid(Dimension[] path) {
        super();
        setDimensions(path);
    }

    /**
     * Dimensions must be configured!
     * 
     * @param path
     */
    public void setDimensions(Dimension[] path) {
        for (Dimension d : path) {
            this.path.add(d.getName());
            dimensions.add(d);
            keyLen += d.getKeyLen();
        }
    }

    public Cube getParentCube() {
        return parentCube;
    }

    public void setParentCube(Cube parentCube) {
        this.parentCube = parentCube;
    }

    public int getHbaseTTL() {
        return hbaseTTL;
    }

    public void setHbaseTTL(int hbaseTimeToLiveSeconds) {
        this.hbaseTTL = hbaseTimeToLiveSeconds;
    }

    public boolean isHbaseInMemory() {
        return hbaseInMemory;
    }

    public void setHbaseInMemory(boolean hbaseInMem) {
        this.hbaseInMemory = hbaseInMem;
    }

    public int getHbaseMaxVersions() {
        return hbaseMaxVersions;
    }

    public void setHbaseMaxVersions(int hbaseMaxVersions) {
        this.hbaseMaxVersions = hbaseMaxVersions;
    }

    @Override
    public int getKeyLen() {
        return keyLen;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }
    
    public String getCompilerGroup() {
        return compilerGroup;
    }

    public void setCompilerGroup(String compilerGroup) {
        this.compilerGroup = compilerGroup;
    }

    @Override
    public List<String> getCuboidPath() {
        return unmodifiablePath;
    }

    @Override
    public List<Dimension> getCuboidDimensions() {
        return unmodifiableDimensions;
    }

    @Override
    public String getCuboidTableName() throws IOException {
        // so let's control it length by computing both verbal hint and
        // hash part which is unique but not human-readable.
        Validate.notNull(tablePrefix);
        try {

            StringBuffer hint = new StringBuffer(tablePrefix);
            java.security.MessageDigest md5 = MessageDigest.getInstance("MD5");

            for (String dimName : getCuboidPath()) {

                hint.append(dimName.charAt(0));
                hint.append(dimName.charAt(dimName.length() - 1));
                md5.update(dimName.getBytes("utf-8"));
            }
            byte[] id = new byte[32];
            HblUtil.fillCompositeKeyWithHex(md5.digest(), 0, 16, id, 0);
            hint.append("_");
            hint.append(new String(id));
            return hint.toString();
        } catch (NoSuchAlgorithmException exc) {
            throw new IOException("jsse doesn't support MD5 algo.");
        }
    }

}
