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
package com.inadco.hbl.piggybank;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Dimension;
import com.inadco.hbl.api.Hierarchy;
import com.inadco.hbl.util.HblUtil;

/**
 * Expects a series of parameters. first parameter is cuboid path. all others
 * are dimension values per cuboid spec.
 * 
 * Returns a bag of possible cuboid keys. More than one key may be returned if
 * hierarchy is present, it will generate a new key version for every hierarchy
 * key
 * 
 * @author dmitriy
 * 
 */
public class Dimensions2CuboidKey extends BaseFunc<DataBag> {

    private Map<String, Cuboid> path2CuboidMap = new HashMap<String, Cuboid>();

    public Dimensions2CuboidKey(String encodedModel) {
        super(encodedModel);
        for (Cuboid c : cube.getCuboids())
            path2CuboidMap.put(HblUtil.encodeCuboidPath(c), c);

    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        Validate.notNull(input, "input cannot be null");

        String cuboidPath = (String) input.get(0);
        Validate.notNull(cuboidPath, "path parameter can't be null");

        Cuboid c = path2CuboidMap.get(cuboidPath);
        Validate.notNull(c, "cuboid not found");

        DataBag db = new DefaultDataBag();

        byte[] key = new byte[c.getKeyLen()];
        walkDimensions(input, db, key, c.getCuboidDimenions(), 0, 0, TupleFactory.getInstance());

        return db;
    }

    private void walkDimensions(Tuple input,
                                DataBag holder,
                                byte[] keyHolder,
                                List<Dimension> dimensions,
                                int dimensionIndex,
                                int keyOffset,
                                TupleFactory tf) throws ExecException {

        Dimension d = dimensions.get(dimensionIndex);
        if (d instanceof Hierarchy) {
            Hierarchy h = (Hierarchy) d;
            int depth = h.getDepth();
            for (int i = 0; i < depth; i++) {
                h.getKey(input.get(dimensionIndex + 1), i, keyHolder, keyOffset);
                if (dimensions.size() == dimensionIndex + 1) {
                    holder.add(tf.newTuple(new DataByteArray(keyHolder)));
                    keyHolder = keyHolder.clone();
                } else
                    walkDimensions(input,
                                   holder,
                                   keyHolder,
                                   dimensions,
                                   dimensionIndex + 1,
                                   keyOffset + d.getKeyLen(),
                                   tf);

            }
        } else {
            // non-hierarchy
            d.getKey(input.get(dimensionIndex + 1), keyHolder, keyOffset);
            if (dimensions.size() == dimensionIndex + 1) {
                holder.add(tf.newTuple(new DataByteArray(keyHolder)));
                keyHolder = keyHolder.clone();
            } else
                walkDimensions(input, holder, keyHolder, dimensions, dimensionIndex + 1, keyOffset + d.getKeyLen(), tf);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {

        try {
            Validate.isTrue(input.size() > 1, "insufficient input size");
            Validate.isTrue(input.getField(0).type == DataType.CHARARRAY, "path must be a string");
            return SchemaUtil.newBagSchema(new Byte[] { DataType.BYTEARRAY });
        } catch (FrontendException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public Type getReturnType() {
        return DataByteArray.class;
    }

}
