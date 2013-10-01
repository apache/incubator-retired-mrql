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

import org.apache.mrql.gen.Trees;
import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/** A FileInputFormat for text files (CVS, XML, JSON, ...) */
final public class MapReduceParsedInputFormat extends MapReduceMRQLFileInputFormat {

    public static class ParsedRecordReader extends RecordReader<MRContainer,MRContainer> {
        final FSDataInputStream fsin;
        final long start;
        final long end;
        Iterator<MRData> result;
        MRData data;
        Parser parser;

        public ParsedRecordReader ( FileSplit split,
                                    TaskAttemptContext context,
                                    Class<? extends Parser> parser_class,
                                    Trees args ) throws IOException {
            Configuration conf = context.getConfiguration();
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(split.getPath());
            try {
                parser = parser_class.newInstance();
            } catch (Exception ex) {
                throw new Error("Unrecognized parser:"+parser_class);
            };
            parser.initialize(args);
            parser.open(fsin,start,end);
            result = null;
        }

        public boolean nextKeyValue () throws IOException {
            while (result == null || !result.hasNext()) {
                String s = parser.slice();
                if (s == null)
                    return false;
                result = parser.parse(s).iterator();
            };
            data = (MRData)result.next();
            return true;
        }

        public MRContainer getCurrentKey () throws IOException {
            return new MRContainer(new MR_long(fsin.getPos()));
        }

        public MRContainer getCurrentValue () throws IOException {
            return new MRContainer(data);
        }

        public long getPos () throws IOException { return fsin.getPos(); }

        public void close () throws IOException { fsin.close(); }

        public float getProgress () throws IOException {
            if (end == start)
                return 0.0f;
            else return Math.min(1.0f, (getPos() - start) / (float)(end - start));
        }

        public void initialize ( InputSplit split, TaskAttemptContext context ) throws IOException { }
    }

    public RecordReader<MRContainer,MRContainer>
              createRecordReader ( InputSplit split,
                                   TaskAttemptContext context ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String path = ((FileSplit)split).getPath().toString();
        ParsedDataSource ds = (ParsedDataSource)DataSource.get(path,conf);
        return new ParsedRecordReader((FileSplit)split,context,ds.parser,(Trees)ds.args);
    }

    /** Find the parser associated with each file in the path and parse the file,
     *  inserting all results into a Bag. The Bag is lazily constructed.
     * @param path the path directory with the files
     * @return a Bag that contains all data
     */
    public Bag materialize ( final Path path ) throws IOException {
        Configuration conf = Plan.conf;
        ParsedDataSource ds = (ParsedDataSource)DataSource.get(path.toString(),conf);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fsin = fs.open(path);
        Parser p;
        try {
            p = ds.parser.newInstance();
        } catch (Exception ex) {
            throw new Error("Unrecognized parser:"+ds.parser);
        };
        final Parser parser = p;
        parser.initialize(ds.args);
        parser.open(fsin,0,Long.MAX_VALUE);
        return new Bag(new BagIterator () {
                Iterator<MRData> iter;
                public boolean hasNext () {
                    while (iter == null || !iter.hasNext()) {
                        String line = parser.slice();
                        if (line == null)
                            return false;
                        iter = parser.parse(line).iterator();
                    };
                    return true;
                }
                public MRData next () {
                    return iter.next();
                }
            });
    }
}
