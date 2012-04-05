package org.inadco.hblr.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class HblrScratchpad {

    public SequenceFile.Writer createSeqFileWriter(String hdfsPath, Class<?> keyClass, Class<?> valClass, 
                                                   CompressionType ct, 
                                                   CompressionCodec codec) throws Exception {
        
        if ( keyClass==null) keyClass=Text.class;
        if ( valClass==null) valClass=BytesWritable.class;
        if ( ct == null ) 
            ct = CompressionType.BLOCK;
        if ( codec == null ) 
            codec = new GzipCodec();
        
        Configuration conf = new Configuration();
        Path p = new Path(hdfsPath);
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer w = SequenceFile.createWriter(fs,conf,p,keyClass,valClass,ct,codec);
        
        Descriptors.Descriptor d=null;
        d.getFullName();
        
        for ( FieldDescriptor fd:d.getFields()) { 
            fd.isRepeated();
            fd.getName();
        
            DynamicMessage.Builder bldr = DynamicMessage.newBuilder(d);
//            bldr.setField
//            DynamicMessage.parseFrom
        }
        
        
        
        return w;

    }
}
