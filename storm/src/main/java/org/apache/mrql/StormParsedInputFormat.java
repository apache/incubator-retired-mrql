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

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.mrql.gen.Trees;

public class StormParsedInputFormat extends StormMRQLFileInputFormat{
    public static class ParsedRecordReader implements RecordReader<MRContainer,MRContainer> {
        final FSDataInputStream fsin;
        final long start;
        final long end;
        Iterator<MRData> result;
        Parser parser;

        public ParsedRecordReader ( FileSplit split,
            Configuration conf,
            Class<? extends Parser> parser_class,
            Trees args ) throws IOException {
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
            value.set((MRData)result.next());
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
    
    
    @Override
    public RecordReader<MRContainer, MRContainer> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        StormEvaluator.load_source_dir();  // load the parsed source parameters from a file
        String path = ((FileSplit)split).getPath().toString();
        ParsedDataSource ds = (ParsedDataSource)DataSource.get(path,Plan.conf);
        return new ParsedRecordReader((FileSplit)split,job,ds.parser,(Trees)ds.args);
    }
    
}
