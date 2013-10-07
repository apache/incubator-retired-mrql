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
import java.io.StringReader;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import java_cup.runtime.Symbol;


/** The JSON parser */
public class JsonFormatParser implements Parser {
    String[] tags;          // split tags
    JsonSplitter splitter;

    public void initialize ( Trees args ) {
        try {
            if (args.length() > 0) {
                if (!(args.nth(0) instanceof Node)
                    || !(((Node)args.nth(0)).name().equals("list")
                         || ((Node)args.nth(0)).name().equals("bag")))
                    throw new Error("Must provide a bag of synchronization property names to split the JSON source: "+args.nth(0));
                Trees ts = ((Node)args.nth(0)).children();
                if (ts.length() == 0)
                    throw new Error("Expected at least one synchronization tagname in JSON source: "+ts);
                tags = new String[ts.length()];
                for ( int i = 0; i < tags.length; i++ )
                    if (ts.nth(i) instanceof StringLeaf)
                        tags[i] = ((StringLeaf)(ts.nth(i))).value();
                    else throw new Error("Expected a synchronization tagname in JSON source: "+ts.nth(i));
            }
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public void open ( String file ) {
        try {
            splitter = new JsonSplitter(tags,file,new DataOutputBuffer());
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public void open ( FSDataInputStream fsin, long start, long end ) {
        try {
            splitter = new JsonSplitter(tags,fsin,start,end,new DataOutputBuffer());
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public Tree type () { return new VariableLeaf("JSON"); }

    public String slice () {
        if (splitter.hasNext()) {
            DataOutputBuffer b = splitter.next();
            return new String(b.getData(),0,b.getLength());
        } else return null;
    }

    public Bag parse ( String s ) {
        try {
            JSONLex scanner = new JSONLex(new StringReader(s));
            JSONParser parser = new JSONParser(scanner);
            Symbol sym = parser.parse();
            return new Bag((MRData)sym.value);
        } catch (Exception e) {
            System.err.println(e);
            return new Bag();
        }
    }
}
