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

/**
 * Class that supports irregular sample (time-based) facts
 * 
 * @author dmitriy
 * 
 */
public class IrregularSample {
    private Object fact;
    private long   time; // time in ms since epoch

    public IrregularSample(Object fact, long time) {
        super();
        this.fact = fact;
        this.time = time;
    }

    public IrregularSample() {
        super();
    }

    public Object getFact() {
        return fact;
    }

    public void setFact(Object fact) {
        this.fact = fact;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

}
