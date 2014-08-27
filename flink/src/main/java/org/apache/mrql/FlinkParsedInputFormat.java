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

import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileInputSplit;


/** A FileInputFormat for text files (CVS, XML, JSON, ...) */
final public class FlinkParsedInputFormat extends FlinkMRQLFileInputFormat {
    public FlinkParsedInputFormat () {}

    public static final class ParsedInputFormat extends FileInputFormat<FData> {
        String pathname;
        transient FlinkParser parser;
        transient Iterator<MRData> iter;
        boolean eof;
        String data_sources; // the encoded data source directory (to be serialized and distributed to clients)

        public ParsedInputFormat ( String pathname ) {
            this.pathname = pathname;
            data_sources = encode_data_sources();
        }

        @Override
        public void open ( FileInputSplit split ) throws IOException {
            super.open(split);
            restore_data_sources(data_sources);
            ParsedDataSource ds = (ParsedDataSource)DataSource.get(split.getPath().toString(),Plan.conf);
            try {
                parser = (FlinkParser)ds.parser.newInstance();
            } catch (Exception ex) {
                throw new Error("Unrecognized parser: "+ds.parser);
            };
            parser.initialize(ds.args);
            parser.open(stream,splitStart,splitStart+splitLength-1);
            eof = false;
        }

        @Override
        public FData nextRecord ( FData data ) throws IOException {
            while (!eof && (iter == null || !iter.hasNext())) {
                String line = parser.slice();
                if (line == null) {
                    eof = true;
                    return null;
                };
                iter = parser.parse(line).iterator();
            };
            return new FData(iter.next());
        }

        @Override
        public boolean reachedEnd () {
            return eof;
        }
    }

    /** the Flink input format for this input */
    public FileInputFormat<FData> inputFormat ( String path ) {
        return new ParsedInputFormat(path);
    }
}
