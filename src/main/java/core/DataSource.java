/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql;

import org.apache.mrql.gen.*;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;


/** A DataSource is any input data source, such as a text file, a key/value map, a data base, an intermediate file, etc */
public class DataSource {
    final public static String separator = "%%%";
    public static DataSourceDirectory dataSourceDirectory = new DataSourceDirectory();
    public static ParserDirectory parserDirectory = new ParserDirectory();
    private static boolean loaded = false;

    public String path;
    public Class<? extends MRQLFileInputFormat> inputFormat;
    public int source_num;
    public boolean to_be_merged;    // if the path is a directory with multiple files, merge them

    final static class ParserDirectory extends HashMap<String,Class<? extends Parser>> {
    }

    /** A dictionary that maps data source paths to DataSource data.
     * It assumes that each path can only be associated with a single data source format and parser
     */
    final static class DataSourceDirectory extends HashMap<String,DataSource> {
        public void read ( Configuration conf ) {
            clear();
            for ( String s: conf.get("mrql.data.source.directory").split("@@@") ) {
                String[] p = s.split("===");
                put(p[0],DataSource.read(p[1],conf));
            }
        }

        public String toString () {
            String s = "";
            for ( String k: keySet() )
                s += "@@@"+k+"==="+get(k);
            if (s.equals(""))
                return s;
            else return s.substring(3);
        }

        public DataSource get ( String name ) {
            for ( Map.Entry<String,DataSource> e: entrySet() )
                if (name.startsWith(e.getKey()))
                    return e.getValue();
            return null;
        }

        public void distribute ( Configuration conf ) {
            conf.set("mrql.data.source.directory",toString());
        }
    }

    DataSource () {}

    DataSource ( int source_num,
                 String path,
                 Class<? extends MRQLFileInputFormat> inputFormat,
                 Configuration conf ) {
        this.source_num = source_num;
        this.path = path;
        this.inputFormat = inputFormat;
        to_be_merged = false;
        try {
            Path p = new Path(path);
            FileSystem fs = p.getFileSystem(conf);
            String complete_path = fs.getFileStatus(p).getPath().toString();
            //String complete_path = "file:"+path;
            this.path = complete_path;
            dataSourceDirectory.put(this.path,this);
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    public static void loadParsers() {
        if (!loaded) {
            DataSource.parserDirectory.put("xml",XMLParser.class);
            DataSource.parserDirectory.put("json",JsonFormatParser.class);
            DataSource.parserDirectory.put("line",LineParser.class);
            loaded = true;
        }
    }

    static {
        loadParsers();
    }

    private static long size ( Path path, Configuration conf ) throws IOException {
        FileStatus s = path.getFileSystem(conf).getFileStatus(path);
        if (!s.isDir())
            return s.getLen();
        long size = 0;
        for ( FileStatus fs: path.getFileSystem(conf).listStatus(path) )
            size += fs.getLen();
        return size;
    }

    /** data set size in bytes */
    public long size ( Configuration conf ) {
        try {
            return size(new Path(path),conf);
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    public static DataSource read ( String buffer, Configuration conf ) {
        try {
            String[] s = buffer.split(separator);
            int n = Integer.parseInt(s[1]);
            if (s[0].equals("Binary"))
                return new BinaryDataSource(n,s[2],conf);
            else if (s[0].equals("Generator"))
                return new GeneratorDataSource(n,s[2],conf);
            else if (s[0].equals("Text"))
                return new ParsedDataSource(n,s[3],parserDirectory.get(s[2]),((Node)Tree.parse(s[4])).children(),conf);
            else throw new Error("Unrecognized data source: "+s[0]);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public static DataSource get ( String path, Configuration conf ) {
        if (dataSourceDirectory.isEmpty())
            dataSourceDirectory.read(conf);
        return dataSourceDirectory.get(path);
    }

    public static DataSource getCached ( String remote_path, String local_path, Configuration conf ) {
        DataSource ds = get(remote_path,conf);
        ds.path = local_path;
        dataSourceDirectory.put(local_path,ds);
        return ds;
    }

    /** return the first num values */
    public List<MRData> take ( int num ) {
        int count = num;
        try {
            ArrayList<MRData> res = new ArrayList<MRData>();
            Iterator<MRData> it = inputFormat.newInstance().materialize(new Path(path)).iterator();
            for ( int i = num; (num < 0 || i > 0) && it.hasNext(); i-- )
                if (Config.hadoop_mode && Config.bsp_mode)
                    res.add(((Tuple)it.next()).get(1));  // strip tag in BSP mode
                else res.add(it.next());
            return res;
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    static Tuple tuple_container = new Tuple(new Tuple(),new Tuple());

    /** accumulate all datasource values */
    public MRData reduce ( MRData zero, final Function acc ) {
        try {
            MRData res = zero;
            for ( MRData x: inputFormat.newInstance().materialize(new Path(path)) ) {
                if (Config.hadoop_mode && Config.bsp_mode)
                    x = ((Tuple)x).get(1); // strip tag in BSP mode
                tuple_container.set(0,res).set(1,x);
                res = acc.eval(tuple_container);
            };
            return res;
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }
}
