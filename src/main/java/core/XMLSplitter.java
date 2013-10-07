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
import java.util.Iterator;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.*;
import java.io.*;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.fs.FSDataInputStream;


/** Extract the XML elements tagged by tags from a data split of the input stream (fsin)
 * and store them in a buffer (to be parsed by SAX).
 */
final public class XMLSplitter implements Iterator<DataOutputBuffer> {
    boolean in_memory;
    FSDataInputStream fsin; // for HDFS processing
    BufferedReader in;      // for in-memory processing
    String[] tags;
    long start;
    long end;
    StringBuffer tagname = new StringBuffer(100);
    String start_tagname;
    final DataOutputBuffer buffer;

    XMLSplitter ( String[] tags, FSDataInputStream fsin, long start, long end,
                  DataOutputBuffer buffer ) {
        in_memory = false;
        this.tags = tags;
        this.fsin = fsin;
        this.start = start;
        this.end = end;
        this.buffer = buffer;
        try {
            fsin.seek(start);
        } catch ( IOException e ) {
            System.err.println("*** Cannot parse the data split: "+fsin);
        }
    }

    XMLSplitter ( String[] tags, String file, DataOutputBuffer buffer ) {
        in_memory = true;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(file)),
                                    100000);
        } catch ( Exception e ) {
            throw new Error("Cannot open the file: "+file);
        };
        this.tags = tags;
        this.buffer = buffer;
    }

    public boolean hasNext () {
        try {
            if (in_memory || fsin.getPos() < end)
                if (skip())
                    return store();
            return false;
        } catch (Exception e) {
            System.err.println(e);
            return false;
        }
    }

    public DataOutputBuffer next () {
        return buffer;
    }

    public void remove () { }

    boolean is_start_tag () {
        if (tags == null)
            return true;
        for (String tag: tags)
            if (tag.contentEquals(tagname))
                return true;
        return false;
    }

    char read_tag () throws IOException {
        tagname.setLength(0);
        while (true) {
            int b = in_memory ? in.read() : fsin.read();
            if (b == -1)
                return ' ';
            else if (!Character.isLetterOrDigit(b) && b != ':' && b != '_')
                return (char)b;
            tagname.append((char)b);
        }
    }

    /** skip until the beginning of a split element */
    boolean skip () throws IOException {
        while (true) {
            int b = in_memory ? in.read() : fsin.read();
            if (b == -1 || (!in_memory && fsin.getPos() >= end))
                return false;
            else if (b == '<') {
                b = read_tag();
                if (is_start_tag()) {
                    buffer.reset();
                    buffer.write('<');
                    for ( int i = 0; i < tagname.length(); i++ )
                        buffer.write(tagname.charAt(i));
                    buffer.write(b);
                    start_tagname = new String(tagname);
                    return true;
                }
            }
        }
    }

    /** store one split element into the buffer; may cross split boundaries */
    boolean store () throws IOException {
        while (true) {
            int b = in_memory ? in.read() : fsin.read();
            if (b == -1)
                return false;
            if (b == '&') {  // don't validate external XML entities
                buffer.write('&');buffer.write('a');buffer.write('m');buffer.write('p');buffer.write(';');
            } else buffer.write(b);
            if (b == '<') {
                b = in_memory ? in.read() : fsin.read();
                buffer.write(b);
                if (b == '/') {
                    b = read_tag();
                    for ( int i = 0; i < tagname.length(); i++ )
                        buffer.write(tagname.charAt(i));
                    buffer.write(b);
                    if (start_tagname.contentEquals(tagname)) {
                        while (b != '>') {
                            b = fsin.read();
                            buffer.write(b);
                        };
                        return true;
                    }
                }
            }
        }
    }
}
