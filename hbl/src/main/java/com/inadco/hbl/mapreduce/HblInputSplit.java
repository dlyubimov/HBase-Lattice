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
package com.inadco.hbl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

class HblInputSplit extends InputSplit {
    private String regionLocation;
    private byte[] startGroupingKey;
    private byte[] endGroupingKey;

    HblInputSplit(String regionLocation, byte[] startGroupingKey, byte[] endGroupingKey) {
        super();
        this.regionLocation = regionLocation;
        this.startGroupingKey = startGroupingKey;
        this.endGroupingKey = endGroupingKey;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        // ??
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] { regionLocation };
    }

    public String getRegionLocation() {
        return regionLocation;
    }

    public byte[] getStartGroupingKey() {
        return startGroupingKey;
    }

    public byte[] getEndGroupingKey() {
        return endGroupingKey;
    }

}
