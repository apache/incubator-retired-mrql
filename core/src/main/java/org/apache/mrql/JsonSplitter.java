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
import java_cup.runtime.Symbol;
import java.util.Iterator;
import java.io.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;


/** Extract the JSON objects tagged by tags from a data split of the input stream (fsin) */
final public class JsonSplitter implements Iterator<DataOutputBuffer> {
    boolean in_memory;
    FSDataInputStream fsin; // for HDFS processing
    InputStream in;         // for in-memory processing
    JSONLex scanner;
    String[] tags;
    long start;
    long end;
    final DataOutputBuffer buffer;

    JsonSplitter ( String[] tags, FSDataInputStream fsin, long start, long end,
                   DataOutputBuffer buffer ) {
        in_memory = false;
        this.tags = tags;
        this.fsin = fsin;
        this.end = end;
        this.buffer = buffer;
        try {
            fsin.seek(start);
            this.start = (start == 0) ? start : sync(start);
            fsin.seek(this.start);
            scanner = new JSONLex(fsin);
        } catch ( IOException e ) {
            System.err.println("*** Cannot parse the data split: "+fsin);
        }
    }

    JsonSplitter ( String[] tags, String file, DataOutputBuffer buffer ) {
        in_memory = true;
        try {
            in = new FileInputStream(file);
        } catch ( Exception e ) {
            throw new Error("Cannot open the file: "+file);
        };
        this.tags = tags;
        this.buffer = buffer;
        scanner = new JSONLex(in);
    }

    private long sync ( long start ) {
        try {
            long first_quote = -1;
            for ( long offset = 0; ; offset++ ) {
                char c = (char)fsin.read();
                if (c == '\"') {
                    if (first_quote >= 0)
                        if ((char)fsin.read() == ':')
                            return start+first_quote;
                    first_quote = offset;
                }
            }
        } catch (IOException ex) {
            return (long)0;
        }
    }

    public boolean hasNext () {
        try {
            if (in_memory || start+scanner.char_pos() < end)
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

    boolean is_start_tag ( String tagname ) {
        if (tags == null)
            return true;
        for (String tag: tags)
            if (tag.contentEquals(tagname))
                return true;
        return false;
    }

    /** skip until the beginning of a split element */
    boolean skip () throws IOException {
        while (true) {
            Symbol s = scanner.next_token();
            if (s.sym == jsym.EOF || (!in_memory && start+scanner.char_pos() >= end))
                return false;
            if (s.sym == jsym.STRING && is_start_tag((String)s.value)) {
                String tag = (String)s.value;
                if (scanner.next_token().sym == jsym.COLON) {
                    buffer.reset();
                    buffer.write('{');
                    buffer.write('\"');
                    for ( int i = 0; i < tag.length(); i++ )
                        buffer.write(tag.charAt(i));
                    buffer.write('\"');
                    buffer.write(':');
                    return true;
                }
            }
        }
    }

    /** store one split element into the buffer; may cross split boundaries */
    boolean store () throws IOException {
        int nest = 0;
        while (true) {
            Symbol s = scanner.next_token();
            if (s.sym == jsym.EOF)
                return false;
            if (s.sym == jsym.O_BEGIN || s.sym == jsym.A_BEGIN)
                nest++;
            else if (s.sym == jsym.O_END || s.sym == jsym.A_END)
                nest--;
            String text = scanner.text();
            for ( int i = 0; i < text.length(); i++ )
                buffer.write(text.charAt(i));
            if (nest == 0) {
                buffer.write('}');
                return true;
            }
        }
    }
}
