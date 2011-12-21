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
package com.inadco.hbl.client.impl.functions;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

public abstract class FCustomFunc extends AbstractAggregateFunc {

    private int ordinal;

    protected FCustomFunc(String name, int ordinal) {
        super(name);
        this.ordinal = ordinal;
    }

    protected void saveState(Aggregation.Builder b, Writable source) throws IOException {
        DataOutputBuffer dob = new DataOutputBuffer();
        source.write(dob);
        dob.close();

        /*
         * do we need to explicitly fill in the "holes" in the repeated fields?
         */
        ByteString bsVal = ByteString.copyFrom(dob.getData(), 0, dob.getLength());
        int cnt = b.getCustomStatesCount();
        if (cnt == ordinal)
            b.addCustomStates(bsVal);
        else if (cnt > ordinal)
            b.setCustomStates(ordinal, bsVal);
        else {
            for (int i = cnt; i < ordinal - 1; i++)
                b.addCustomStates(ByteString.EMPTY);
            b.addCustomStates(bsVal);
        }
    }

    protected <T extends Writable> T extractState(Aggregation source, T recipient) throws IOException {

        if (source.getCustomStatesCount() <= ordinal)
            return null;
        ByteString bs = source.getCustomStates(ordinal);
        if (bs.isEmpty())
            return null;

        readState(bs, recipient);
        return recipient;
    }

    protected <T extends Writable> T extractState(Aggregation.Builder source, T recipient) throws IOException {

        if (source.getCustomStatesCount() <= ordinal)
            return null;
        ByteString bs = source.getCustomStates(ordinal);
        if (bs.isEmpty())
            return null;

        readState(bs, recipient);
        return recipient;
    }

    private void readState(ByteString bs, Writable recipient) throws IOException {
        DataInputBuffer dib = new DataInputBuffer();
        dib.reset(bs.toByteArray(), bs.size());
        recipient.readFields(dib);
    }

}
