package com.inadco.hbl.scanner.filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.FilterBase;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.api.Cuboid;
import com.inadco.hbl.api.Range;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.util.HblUtil;

public class CompositeKeyRowFilter extends FilterBase {
    private String           modelStr;
    private String           cuboidPath;
    
    private transient Cube   model;
    private transient Cuboid cuboid;

    public CompositeKeyRowFilter(String modelStr, String cuboidPath, Range[] pathRange) {
        super();
        this.modelStr = modelStr;
        this.cuboidPath = cuboidPath;
    }

    public CompositeKeyRowFilter() {
        super();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO:
        // we of course do not want to ship off the model with
        // each scan request.
        // instead, we probably can initialize from hbase itself
        // once we have proof of concept this stuff is working,
        // we'd just fetch the model needed based on a model key from
        // a system table, perhaps bypassing the YAML serialization by
        // having a writable cache there.
        // -d
        modelStr = in.readUTF();
        cuboidPath = in.readUTF();
        initModel();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(modelStr);
        out.writeUTF(cuboidPath);

    }

    private void initModel() throws IOException {
        model = YamlModelParser.decodeCubeModel(modelStr);
        cuboid = model.findCuboidForPath(HblUtil.decodeCuboidPath(cuboidPath));
        if (cuboid == null)
            throw new IOException("Unable to find specified cuboid in the model.");

    }

}
