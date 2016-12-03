/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class HDFSFileInputStream extends BaseRichSpout{
    private final String directory;
    private final boolean is_binary;
    private HashMap<String,Long> file_modification_times;
    private StormMRQLFileInputFormat input_format;
    final private SData container = new SData();
    private SpoutOutputCollector _collector;
    
    public HDFSFileInputStream(String directory, boolean is_binary,StormMRQLFileInputFormat input_format) {
        this.directory = directory;
        this.is_binary = is_binary;
        this.input_format = input_format;
    }
    
    private ArrayList<String> new_files () {
        try {
            long ct = System.currentTimeMillis();
            Path dpath = new Path(directory);
            final FileSystem fs = dpath.getFileSystem(Plan.conf);
            final FileStatus[] ds
            = fs.listStatus(dpath,
                new PathFilter () {
                    public boolean accept ( Path path ) {
                        return !path.getName().startsWith("_")
                        && !path.getName().endsWith(".type");
                    }
                });
            ArrayList<String> s = new ArrayList<String>();
            for ( FileStatus d: ds ) {
                String name = d.getPath().toString();
                if (file_modification_times.get(name) == null
                 || d.getModificationTime() >  file_modification_times.get(name)) {
                    file_modification_times.put(name,new Long(ct));
                s.add(name);
            }
        };
        return s;
    } catch (Exception ex) {
        throw new Error("Cannot open a new file from the directory "+directory+": "+ex);
    }
}

@Override
public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("inputdata"));
}

@Override
public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.file_modification_times =  new HashMap<String,Long>();
    _collector = collector;
}

@Override
public void nextTuple() {
    try{
        for ( String path: new_files() ) {
            Path filePath = new Path(path);
            Bag value = input_format.materialize(filePath);
            for(MRData val : value){
                _collector.emit(new Values(val));
            }   
        }
    }
    catch(Exception e){
        e.printStackTrace();
    }
}
}
