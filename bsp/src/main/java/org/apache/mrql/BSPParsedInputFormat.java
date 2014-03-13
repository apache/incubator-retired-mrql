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
import org.apache.hama.bsp.*;


/** A FileInputFormat for text files (CVS, XML, JSON, ...) */
final public class BSPParsedInputFormat extends BSPMRQLFileInputFormat {
    public static class ParsedRecordReader implements RecordReader<MRContainer,MRContainer> {
        final FSDataInputStream fsin;
        final long start;
        final long end;
        final int source_number;
        final MRData source_num_data;
        Iterator<MRData> result;
        Parser parser;

        public ParsedRecordReader ( FileSplit split,
                                    BSPJob job,
                                    Class<? extends Parser> parser_class,
                                    int source_number,
                                    Trees args ) throws IOException {
            Configuration conf = BSPPlan.getConfiguration(job);
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
            this.source_number = source_number;
            source_num_data = new MR_int(source_number);
            parser.initialize(args);
            parser.open(fsin,start,end);
            result = null;
        }

        public MRContainer createKey () {
            return new MRContainer();
        }

        public MRContainer createValue () {
            return new MRContainer();
        }

        public synchronized boolean next ( MRContainer key, MRContainer value ) throws IOException {
            while (result == null || !result.hasNext()) {
                String s = parser.slice();
                if (s == null)
                    return false;
                result = parser.parse(s).iterator();
            };
            value.set(new Tuple(source_num_data,(MRData)result.next()));
            key.set(new MR_long(fsin.getPos()));
            return true;
        }

        public synchronized long getPos () throws IOException { return fsin.getPos(); }

        public synchronized void close () throws IOException { fsin.close(); }

        public float getProgress () throws IOException {
            if (end == start)
                return 0.0f;
            else return Math.min(1.0f, (getPos() - start) / (float)(end - start));
        }
    }

    public RecordReader<MRContainer,MRContainer>
              getRecordReader ( InputSplit split,
                                BSPJob job ) throws IOException {
        Configuration conf = BSPPlan.getConfiguration(job);
        String path = ((FileSplit)split).getPath().toString();
        ParsedDataSource ds = (ParsedDataSource)DataSource.get(path,conf);
        return new ParsedRecordReader((FileSplit)split,job,ds.parser,ds.source_num,(Trees)ds.args);
    }
}
