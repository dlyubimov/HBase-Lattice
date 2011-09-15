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
package com.inadco.hbl.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.Resource;

import com.inadco.hbl.api.Cube;
import com.inadco.hbl.compiler.YamlModelParser;
import com.inadco.hbl.util.IOUtil;

public class HblQueryClient {
    
    private Configuration conf;
    private String encodedYamlStr;
    private String yamlModelStr;
    
    private Cube cube;
    
    public HblQueryClient(Configuration conf, Resource yamlModel) throws IOException {
        super();
        Validate.notNull(conf);
        Validate.notNull(yamlModel);
        
        Deque<Closeable> closeables =new ArrayDeque<Closeable>();
        try { 
        
        
        this.conf = conf;
        
        InputStream is = yamlModel.getInputStream();
        
        Validate.notNull(is);
        closeables.addFirst(is);
        
        yamlModelStr=IOUtil.fromStream(is, "utf-8");
        encodedYamlStr=YamlModelParser.encodeCubeModel(yamlModelStr);
        cube=YamlModelParser.parseYamlModel(yamlModelStr);
        
        } finally { 
            IOUtil.closeAll(closeables);
        }
        
    }
    
    

}
