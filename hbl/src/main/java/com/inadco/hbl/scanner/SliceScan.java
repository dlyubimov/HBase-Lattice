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
package com.inadco.hbl.scanner;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

public class SliceScan {

    private Cube           cube;
    private Cuboid         cuboid;
    private Range[]        slicing;
    private SliceOperation operation;

    public SliceScan(Cube cube, Cuboid cuboid, Range[] slicing, SliceOperation operation) {
        super();
        this.cube = cube;
        this.cuboid = cuboid;
        this.slicing = slicing;
        this.operation = operation;
    }

    public Aggregation performScan() {
        // TODO
        return null;
    }

}
