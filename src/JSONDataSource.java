/*----------------------------------------------------------------------------------------------------
   Copyright 2011-2012 Leonidas Fegaras, University of Texas at Arlington

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   The DataSource for JSON documents
   Programmer: Leonidas Fegaras
   Email: fegaras@cse.uta.edu
   Web: http://lambda.uta.edu/
   Creation: 04/08/11, last update: 10/08/11

*----------------------------------------------------------------------------------------------------*/

package hadoop.mrql;

import Gen.*;
import java_cup.runtime.*;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;


// Extract the JSON objects tagged by tags from a data split of the input stream (fsin)
final class JSONSplitter implements Iterator<DataOutputBuffer> {
    boolean in_memory;
    FSDataInputStream fsin; // for HDFS processing
    InputStream in;         // for in-memory processing
    JSONLex scanner;
    String[] tags;
    long start;
    long end;
    final DataOutputBuffer buffer;

    JSONSplitter ( String[] tags, FSDataInputStream fsin, long start, long end,
		  DataOutputBuffer buffer ) {
	in_memory = false;
	this.tags = tags;
	this.fsin = fsin;
	this.start = start;
	this.end = end;
	this.buffer = buffer;
	scanner = new JSONLex(fsin);
	try {
	    fsin.seek(start);
	} catch ( IOException e ) {
	    System.err.println("*** Cannot parse the data split: "+fsin);
	}
    }

    JSONSplitter ( String[] tags, String file, DataOutputBuffer buffer ) {
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

    boolean is_start_tag ( String tagname ) {
	for (String tag: tags)
	    if (tag.contentEquals(tagname))
		return true;
	return false;
    }

    // skip until the beginning of a split element
    boolean skip () throws IOException {
	while (true) {
	    Symbol s = scanner.next_token();
	    if (s.sym == jsym.EOF || (!in_memory && fsin.getPos() >= end))
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

    // store one split element into the buffer; may cross split boundaries
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


// An JSON parser
class JsonParser implements Parser {
    String[] tags;          // split tags
    JSONSplitter splitter;

    public void initialize ( Trees args ) {
	try {
	    if (args.length() > 0) {
		if (!(args.nth(0) instanceof Node)
		    || !(((Node)args.nth(0)).name().equals("list")
			 || ((Node)args.nth(0)).name().equals("bag")))
		    throw new Error("Expected a bag of synchronization tagnames in JSON source: "+args.nth(0));
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
	    splitter = new JSONSplitter(tags,file,new DataOutputBuffer());
	} catch (Exception e) {
	    throw new Error(e);
	}
    }

    public void open ( FSDataInputStream fsin, long start, long end ) {
	try {
	    splitter = new JSONSplitter(tags,fsin,start,end,new DataOutputBuffer());
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
	    JSONParser parser = new JSONParser();
	    JSONLex scanner = new JSONLex(new StringReader(s));
	    MRQLLex.reset();
	    parser.setScanner(scanner);
	    parser.parse();
	    return new Bag(parser.top_level);
	} catch (Exception e) {
	    System.err.println(e);
	    return new Bag();
	}
    }
}
