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
package com.inadco.hbl.client.impl;

/**
 * Formal slice specification (high level.)
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class Slice {
    private Object  leftBound;
    private Object  rightBound;
    private boolean leftOpen;
    private boolean rightOpen;

    public Slice(Object leftBound, boolean leftOpen, Object rightBound, boolean rightOpen) {
        super();
        this.leftBound = leftBound;
        this.leftOpen = leftOpen;
        this.rightBound = rightBound;
        this.rightOpen = rightOpen;
    }

    public Object getLeftBound() {
        return leftBound;
    }

    public void setLeftBound(Object leftBound) {
        this.leftBound = leftBound;
    }

    public Object getRightBound() {
        return rightBound;
    }

    public void setRightBound(Object rightBound) {
        this.rightBound = rightBound;
    }

    public boolean isLeftOpen() {
        return leftOpen;
    }

    public void setLeftOpen(boolean leftOpen) {
        this.leftOpen = leftOpen;
    }

    public boolean isRightOpen() {
        return rightOpen;
    }

    public void setRightOpen(boolean rightOpen) {
        this.rightOpen = rightOpen;
    }

}
