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
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.*;
import java.io.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;


/** An XML parser */
final public class XMLParser implements Parser {
    String[] tags;          // split tags
    Tree xpath;             // XPath query for fragmentation
    XMLSplitter splitter;
    XPathParser parser;
    XMLReader xmlReader;
    final static SAXParserFactory factory = SAXParserFactory.newInstance();

    public void initialize ( Trees args ) {
        try {
            if (args.length() > 0) {
                if (!(args.nth(0) instanceof Node)
                    || !(((Node)args.nth(0)).name().equals("list")
                         || ((Node)args.nth(0)).name().equals("bag")))
                    throw new Error("Expected a bag of synchronization tagnames to split the XML source: "+args.nth(0));
                Trees ts = ((Node)args.nth(0)).children();
                if (ts.length() == 0)
                    throw new Error("Expected at least one synchronization tagname in XML source: "+ts);
                tags = new String[ts.length()];
                for ( int i = 0; i < tags.length; i++ )
                    if (ts.nth(i) instanceof StringLeaf)
                        tags[i] = ((StringLeaf)(ts.nth(i))).value();
                    else throw new Error("Expected a synchronization tagname in XML source: "+ts.nth(i));
                if (args.length() == 2)
                    xpath = ((Node)args.nth(1)).children().nth(0);
                else xpath = new VariableLeaf("dot");
            } else xpath = new VariableLeaf("dot");
            parser = new XPathParser(xpath);
            factory.setValidating(false);
            factory.setNamespaceAware(false);
            xmlReader = factory.newSAXParser().getXMLReader();
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public void open ( String file ) {
        try {
            splitter = new XMLSplitter(tags,file,new DataOutputBuffer());
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public void open ( FSDataInputStream fsin, long start, long end ) {
        try {
            splitter = new XMLSplitter(tags,fsin,start,end,new DataOutputBuffer());
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public Tree type () { return new VariableLeaf("XML"); }

    public String slice () {
        if (splitter.hasNext()) {
            DataOutputBuffer b = splitter.next();
            return new String(b.getData(),0,b.getLength());
        } else return null;
    }

    public Bag parse ( String s ) {
        try {
            parser.dataConstructor.start();
            xmlReader.setContentHandler(parser.handler);
            xmlReader.parse(new InputSource(new StringReader(s)));
            Bag b = new Bag();
            for ( MRData e: parser.dataConstructor.value() )
                b.add(e);
            return b;
        } catch (Exception e) {
            System.err.println(e);
            return new Bag();
        }
    }
}
