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
import org.apache.flink.core.fs.FSDataInputStream;


/** A parser for line-oriented, character delimited text files (such as CVS) */
final public class FlinkLineParser extends LineParser implements FlinkParser {
    LineReader in;
    byte[] line;

    @Override
    public void open ( FSDataInputStream fsin, long fstart, long fend ) {
        in_memory = false;
        start = fstart;
        end = fend;
        try {
            in = new LineReader(fsin,start,end-start+1,maxLineLength);
            if (false && start != 0) {  // for all but the first data split, skip the first record
                line = in.readLine();
                if (line != null)
                    start += line.length;
            };
            pos = start;
        } catch ( IOException e ) {
            System.err.println("*** Cannot parse the data split: "+fsin);
            start = end;
        }
    }

    @Override
    public String slice () {
        try {
            if (in_memory)
                return buffered_in.readLine();
            while (pos < end) {
                line = in.readLine();
                if (line == null || line.length == 0)
                    return null;
                pos += line.length;
                if (line.length < maxLineLength)
                    return new String(line);
            };
            return null;
        } catch ( Exception e ) {
            System.err.println("*** Cannot slice the text: "+e);
            return "";
        }
    }
}
