/********************************************************************************
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

   File: DataSource.java
   The data sources for MRQL
   Programmer: Leonidas Fegaras, UTA
   Date: 07/06/11 - 03/30/12

********************************************************************************/

package hadoop.mrql;

import Gen.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LineReader;


// for a new text data source, you must implement a new parser
interface Parser {
    public void initialize ( Trees args );
    public Tree type ();
    public void open ( String file );
    public void open ( FSDataInputStream fsin, long start, long end );
    public String slice ();
    public Bag parse ( String s );
}


final class ParserDirectory extends HashMap<String,Class<? extends Parser>> {
}


// A dictionary that maps data source paths to DataSource data.
// It assumes that each path can only be associated with a single data source format and parser
final class DataSourceDirectory extends HashMap<String,DataSource> {
    public void read ( Configuration conf ) {
	for ( String s: conf.get("mrql.data.source.directory").split("@@@") ) {
	    String[] p = s.split("===");
	    put(p[0],DataSource.read(p[1],conf));
	}
    }

    public String toString () {
	String s = "";
	for ( String k: keySet() )
	    s += "@@@"+k+"==="+get(k);
	if (s.equals(""))
	    return s;
	else return s.substring(3);
    }

    public DataSource get ( String name ) {
	for ( Map.Entry<String,DataSource> e: entrySet() )
	    if (name.startsWith(e.getKey()))
		return e.getValue();
	return null;
    }

    public void distribute ( Configuration conf ) {
	conf.set("mrql.data.source.directory",toString());
    }
}


// A DataSource is any input data source, such as a text file, a key/value map, a data base, an intermediate file, etc
public class DataSource {
    final public static String separator = "%%%";
    public static DataSourceDirectory dataSourceDirectory = new DataSourceDirectory();
    public static ParserDirectory parserDirectory = new ParserDirectory();
    private static boolean loaded = false;

    public String path;
    public Class<? extends MRQLFileInputFormat> inputFormat;
    public int source_num;
    public boolean to_be_merged;    // if the path is a directory with multiple files, merge them

    DataSource ( int source_num,
		 String path,
		 Class<? extends MRQLFileInputFormat> inputFormat, Configuration conf ) {
	this.source_num = source_num;
	this.path = path;
	this.inputFormat = inputFormat;
	to_be_merged = false;
	try {
	    Path p = new Path(path);
	    FileSystem fs = p.getFileSystem(conf);
	    String complete_path = fs.getFileStatus(p).getPath().toString();
	    this.path = complete_path;
	    dataSourceDirectory.put(complete_path,this);
	} catch (IOException e) {
	    throw new Error(e);
	}
    }

    public static void loadParsers() {
	if (!loaded) {
	    DataSource.parserDirectory.put("xml",XMLParser.class);
	    DataSource.parserDirectory.put("json",JsonParser.class);
	    DataSource.parserDirectory.put("line",LineParser.class);
	    loaded = true;
	}
    }

    static {
	loadParsers();
    }

    public static long size ( Path path, Configuration conf ) throws IOException {
	FileStatus s = path.getFileSystem(conf).getFileStatus(path);
	if (!s.isDir())
	    return s.getLen();
	long size = 0;
	for ( FileStatus fs: path.getFileSystem(conf).listStatus(path) )
	    size += fs.getLen();
	return size;
    }

    // data set size in bytes
    public long size ( Configuration conf ) {
	try {
	    return size(new Path(path),conf);
	} catch (IOException e) {
	    throw new Error(e);
	}
    }

    public static DataSource read ( String buffer, Configuration conf ) {
	try {
	    String[] s = buffer.split(separator);
	    int n = Integer.parseInt(s[1]);
	    if (s[0].equals("Binary"))
		return new BinaryDataSource(n,s[2],conf);
	    else if (s[0].equals("Generator"))
		return new GeneratorDataSource(n,s[2],conf);
	    else if (s[0].equals("Text"))
		return new ParsedDataSource(n,s[3],parserDirectory.get(s[2]),((Node)Tree.parse(s[4])).children(),conf);
	    else throw new Error("Unrecognized data source: "+s[0]);
	} catch (Exception e) {
	    throw new Error(e);
	}
    }

    public static DataSource get ( String path, Configuration conf ) {
	if (dataSourceDirectory.isEmpty())
	    dataSourceDirectory.read(conf);
	return dataSourceDirectory.get(path);
    }

    public static DataSource getCached ( String remote_path, String local_path, Configuration conf ) {
	DataSource ds = get(remote_path,conf);
	ds.path = local_path;
	dataSourceDirectory.put(local_path,ds);
	return ds;
    }
}


// The domain of the physical algebra is a set of DataSources
final class DataSet {
    public ArrayList<DataSource> source;  // multiple sources
    public long counter;  // a Hadoop user-defined counter used by the `repeat' operator
    public long records;  // total number of dataset records

    DataSet ( DataSource s, long counter, long records ) {
	source = new ArrayList<DataSource>();
	source.add(s);
	this.counter = counter;
	this.records = records;
    }

    DataSet ( long counter, long records ) {
	source = new ArrayList<DataSource>();
	this.counter = counter;
	this.records = records;
    }

    public void add ( DataSource s ) {
	source.add(s);
    }

    public void merge ( DataSet ds ) {
	source.addAll(ds.source);
	counter += ds.counter;
	records += ds.records;
    }

    // dataset size in bytes
    public long size ( Configuration conf ) {
	long n = 0;
	for (DataSource s: source)
	    n += s.size(conf);
	return n;
    }

    public String merge () {
	Object[] ds = source.toArray();
	String path = ((DataSource)ds[0]).path.toString();
	for ( int i = 1; i < ds.length; i++ )
	    path += ","+((DataSource)ds[i]).path;
	return path;
    }

    public String toString () {
	String p = "<"+counter;
	for (DataSource s: source)
	    p += ","+s;
	return p+">";
    }
}


// Used for storing intermediate results and data dumps
final class BinaryDataSource extends DataSource {
    BinaryDataSource ( int source_num, String path, Configuration conf ) {
	super(source_num,path,BinaryInputFormat.class,conf);
    }

    BinaryDataSource ( String path, Configuration conf ) {
	super(-1,path,BinaryInputFormat.class,conf);
    }

    public String toString () {
	return "Binary"+separator+source_num+separator+path;
    }
}


// A data source for a text HDFS file along with the parser to parse it
class ParsedDataSource extends DataSource {
    public Class<? extends Parser> parser;
    public Trees args;

    ParsedDataSource ( int source_num,
		       String path,
		       Class<? extends Parser> parser,
		       Trees args,
		       Configuration conf ) {
	super(source_num,path,ParsedInputFormat.class,conf);
	this.parser = parser;
	this.args = args;
    }

    ParsedDataSource ( String path,
		       Class<? extends Parser> parser,
		       Trees args,
		       Configuration conf ) {
	super(-1,path,ParsedInputFormat.class,conf);
	this.parser = parser;
	this.args = args;
    }

    public String toString () {
	try {
	    String pn = "";
	    for ( String k: DataSource.parserDirectory.keySet() )
		if (DataSource.parserDirectory.get(k).equals(parser))
		    pn = k;
	    return "Text"+separator+source_num+separator+pn+separator+path
		   +separator+(new Node("args",args)).toString();
	} catch (Exception e) {
	    throw new Error(e);
	}
    }
}


// A parser for line-oriented, character delimited text files
final class LineParser implements Parser {
    final static int maxLineLength = 1000;
    boolean in_memory;
    FSDataInputStream fsin;     // for HDFS processing
    LineReader in;
    BufferedReader buffered_in; // for in-memory processing
    Text line;
    long start;
    long end;
    long pos;
    String delimiter;
    Tree type;
    byte[] types;    // a vector of basic type ids (see MRContainer in MapReduceData)
    int type_length;

    public Tree type () {
	return Translate.relational_record_type(type);
    }

    public void initialize ( Trees args ) {
	if (Config.hadoop_mode && Plan.conf == null)
	    Plan.conf = Evaluator.new_configuration();
	if (args.length() != 2)
	    throw new Error("The line parser must have two arguments: "+args);
	if (!(args.nth(0) instanceof StringLeaf))
	    throw new Error("Expected a delimiter: "+args.nth(0));
	delimiter = ((StringLeaf)args.nth(0)).value();
	if (delimiter.length() == 0)
	    throw new Error("Expected a delimiter with at least one character: "+delimiter);
	type = ((Node)args.nth(1)).children().nth(0);
	types = Translate.relational_record(type);
	type_length = 0;
	for ( int i = 0; i < types.length; i++ )
	    if (types[i] >= 0)
		type_length++;
	if (type_length < 1)
	    throw new Error("A relational record type must have at least one component: "
			    +Interpreter.print_type(type));
    }

    public void open ( String file ) {
	in_memory = true;
	try {
	    buffered_in = new BufferedReader(new InputStreamReader(new FileInputStream(file)),
					     10000);
	} catch ( Exception e ) {
	    throw new Error("Cannot open the file: "+file);
	}
    }

    public void open ( FSDataInputStream fsin, long fstart, long fend ) {
	in_memory = false;
	this.fsin = fsin;
	start = fstart;
	end = fend;
	line = new Text();
	try {
	    if (start != 0) {  // for all but the first data split, skip the first record
		--start;
		fsin.seek(start);
		in = new LineReader(fsin,Plan.conf);
		start += in.readLine(new Text(),0,(int) Math.min(Integer.MAX_VALUE,end-start));
	    } else in = new LineReader(fsin,Plan.conf);
	    pos = start;
	} catch ( IOException e ) {
	    System.err.println("*** Cannot parse the data split: "+fsin);
	    this.start = end;
	}
    }

    public String slice () {
	try {
	    if (in_memory)
		return buffered_in.readLine();
	    while (pos < end) {
		int newSize = in.readLine(line,maxLineLength,
					  Math.max((int)Math.min(Integer.MAX_VALUE,end-pos),
						   maxLineLength));
		if (newSize == 0)
		    return null;
		pos += newSize;
		if (newSize < maxLineLength)
		    return line.toString();
	    };
	    return null;
	} catch ( Exception e ) {
	    System.err.println("*** Cannot slice the text: "+e);
	    return "";
	}
    }

    private static MRData parse_value ( String text, byte type ) {
	switch (type) {
	case MRContainer.BYTE: return new MR_byte(Byte.parseByte(text));
	case MRContainer.SHORT: return new MR_short(Short.parseShort(text));
	case MRContainer.INT: return new MR_int(Integer.parseInt(text));
	case MRContainer.LONG: return new MR_long(Long.parseLong(text));
	case MRContainer.FLOAT: return new MR_float(Float.parseFloat(text));
	case MRContainer.DOUBLE: return new MR_double(Double.parseDouble(text));
	case MRContainer.CHAR: return new MR_char(text.charAt(0));
	case MRContainer.STRING: return new MR_string(text);
	};
	System.err.println("*** Cannot parse the type "+MRContainer.type_names[type]+" in '"+text+"'");
	return null;
    }

    public Bag parse ( String line ) {
        try {
	    if (line == null)
		return new Bag();
            Tuple t = new Tuple(type_length);
	    int loc = 0;
	    int j = 0;
            for ( int i = 0; i < types.length; i++ ) {
		int k = line.indexOf(delimiter,loc);
		if (types[i] >= 0) {
		    String s = (k > 0) ? line.substring(loc,k) : line.substring(loc);
		    MRData v = parse_value(s,types[i]);
		    if (v == null)
			return new Bag();
		    t.set(j++,v);
		};
		loc = k+delimiter.length();
		if (k < 0 && i+1 < types.length) {
		    System.err.println("*** Incomplete parsed text line: "+line);
		    return new Bag();
		}
	    };
	    return new Bag(t);
        } catch ( Exception e ) {
            System.err.println("*** Cannot parse the text line: "+line);
            return new Bag();
        }
    }
}


// Used for processing the range min..max
final class GeneratorDataSource extends DataSource {
    GeneratorDataSource ( int source_num, String path, Configuration conf ) {
	super(source_num,path,GeneratorInputFormat.class,conf);
    }

    GeneratorDataSource ( String path, Configuration conf ) {
	super(-1,path,GeneratorInputFormat.class,conf);
    }

    public static long size ( Path path, Configuration conf ) throws IOException {
	// each file generates range_split_size long integers
	FileStatus s = path.getFileSystem(conf).getFileStatus(path);
	if (!s.isDir())
	    return Config.range_split_size*8;
	long size = 0;
	for ( FileStatus fs: path.getFileSystem(conf).listStatus(path) )
	    size += Config.range_split_size*8;
	return size;
    }

    public String toString () {
	return "Generator"+separator+source_num+separator+path;
    }
}
