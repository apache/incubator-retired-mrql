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
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;


/** A superclass for all MRQL FileInputFormats */
abstract public class FlinkMRQLFileInputFormat implements MRQLFileInputFormat {
    public FlinkMRQLFileInputFormat () {}

    private static DataSource read_directory ( String buffer ) {
        try {
            String[] s = buffer.split(DataSource.separator);
            int n = Integer.parseInt(s[1]);
            if (s[0].equals("Binary"))
                return new BinaryDataSource(s[2],Plan.conf);
            else if (s[0].equals("Generator"))
                return new GeneratorDataSource(s[2],Plan.conf);
            else if (s[0].equals("Text"))
                return new FlinkParsedDataSource(s[3],DataSource.parserDirectory.get(s[2]),
                                                 ((Node)Tree.parse(s[4])).children());
            else throw new Error("Unrecognized data source: "+s[0]);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    /** encode the data source directory (to be serialized and distributed to clients) */
    public static String encode_data_sources () {
        return DataSource.dataSourceDirectory.toString();
    }

    /** restore the data source directory */
    public static void restore_data_sources ( String data_sources ) {
        if (Config.local_mode)
            return;
        DataSource.parserDirectory = new DataSource.ParserDirectory();
        DataSource.parserDirectory.put("xml",FlinkXMLParser.class);
        DataSource.parserDirectory.put("json",FlinkJsonParser.class);
        DataSource.parserDirectory.put("line",FlinkLineParser.class);
        DataSource.dataSourceDirectory = new DataSource.DataSourceDirectory();
        for ( String s: data_sources.split("@@@") ) {
            String[] p = s.split("===");
            DataSource.dataSourceDirectory.put(p[0],read_directory(p[1]));
        }
    }

    /** the Flink input format for this input */
    abstract public FileInputFormat<FData> inputFormat ( String path );

    /** materialize the input file into a memory Bag
     * @param path the path directory
     * @return a Bag that contains all data
     */
    public Bag materialize ( final Path path ) throws IOException {
        final FileInputFormat<FData> sf = inputFormat(path.toString());
        final FileInputSplit[] splits = sf.createInputSplits(1);
        if (splits.length == 0)
            return new Bag();
        sf.open(splits[0]);
        return new Bag(new BagIterator () {
                int i = 0;
                FData d = new FData();
                public boolean hasNext () {
                    try {
                        while (sf.reachedEnd() && ++i < splits.length) {
                            sf.close();
                            sf.open(splits[i]);
                        };
                        return i < splits.length;
                    } catch (IOException ex) {
                        throw new Error("Cannot materialize a flink data source into a bag: "+ex);
                    }
                }
                public MRData next () {
                    try {
                        return sf.nextRecord(d).data();
                    } catch (IOException ex) {
                        throw new Error("Cannot materialize a flink data source into a bag: "+ex);
                    }
                }
            });
    }

    /** materialize the entire dataset into a Bag
     * @param x the DataSet in HDFS to collect values from
     * @param strip is not used in MapReduce mode
     * @return the Bag that contains the collected values
     */
    public final Bag collect ( final DataSet x, boolean strip ) throws Exception {
        Bag res = new Bag();
        for ( DataSource s: x.source )
            if (s.to_be_merged)
                res = res.union(((FlinkDataSource)s).merge());
            else res = res.union(s.inputFormat.newInstance().materialize(new Path(s.path)));
        return res;
    }
}
