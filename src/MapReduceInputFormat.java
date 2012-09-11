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

   File: InputFormat.java
   The input format used by MRQL data sources
   Programmer: Leonidas Fegaras, UTA
   Date: 07/06/11 - 03/30/12

********************************************************************************/

package hadoop.mrql;

import Gen.Trees;
import java.util.List;
import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


abstract class MRQLFileInputFormat extends FileInputFormat<MRContainer,MRContainer> {
    public MRQLFileInputFormat () {}

    // record reader for map-reduce
    abstract public RecordReader<MRContainer,MRContainer>
	createRecordReader ( InputSplit split,
			     TaskAttemptContext context ) throws IOException, InterruptedException;

    // materialize the input file into a memory Bag
    abstract Bag materialize ( final Path path ) throws IOException;

    // materialize the entire dataset into a Bag
    public final static Bag collect ( final DataSet x ) throws Exception {
	Bag res = new Bag();
	for ( DataSource s: x.source )
	    if (s.to_be_merged)
		res = res.union(Plan.merge(s));
	    else res = res.union(s.inputFormat.newInstance().materialize(new Path(s.path)));
	return res;
    }
}


final class BinaryInputFormat extends MRQLFileInputFormat {
    final static SequenceFileInputFormat<MRContainer,MRContainer> inputFormat
	                       = new SequenceFileInputFormat<MRContainer,MRContainer>();

    public RecordReader<MRContainer,MRContainer>
	      createRecordReader ( InputSplit split,
				   TaskAttemptContext context ) throws IOException, InterruptedException {
	return inputFormat.createRecordReader(split,context);
    }

    Bag materialize ( final Path path ) throws IOException {
	final FileSystem fs = path.getFileSystem(Plan.conf);
	final FileStatus[] ds
	           = fs.listStatus(path,
				   new PathFilter () {
				       public boolean accept ( Path path ) {
					   return !path.getName().startsWith("_");
				       }
				   });
	if (ds.length > 0)
	    return new Bag(new BagIterator () {
		    SequenceFile.Reader reader = new SequenceFile.Reader(fs,ds[0].getPath(),Plan.conf);
		    MRContainer key = new MRContainer(new MR_int(0));
		    MRContainer value = new MRContainer(new MR_int(0));
		    int i = 1;
		    public boolean hasNext () {
			try {
			    if (reader.next(key,value))
				return true;
			    do {
				if (i >= ds.length)
				    return false;
				reader.close();
				reader = new SequenceFile.Reader(fs,ds[i++].getPath(),Plan.conf);
			    } while (!reader.next(key,value));
			    return true;
			} catch (IOException e) {
			    throw new Error("Cannot collect values from an intermediate result");
			}
		    }
		    public MRData next () {
			return value.data();
		    }
		});
	return new Bag();
    }
}


final class ParsedInputFormat extends MRQLFileInputFormat {
    public static class ParsedRecordReader extends RecordReader<MRContainer,MRContainer> {
	final FSDataInputStream fsin;
	final long start;
	final long end;
	Iterator<MRData> result;
	MRData data;
	Parser parser;

	public ParsedRecordReader ( FileSplit split,
				    TaskAttemptContext context,
				    Class<? extends Parser> parser_class,
				    Trees args ) throws IOException {
	    Configuration conf = context.getConfiguration();
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

	public boolean nextKeyValue () throws IOException {
	    while (result == null || !result.hasNext()) {
		String s = parser.slice();
		if (s == null)
		    return false;
		result = parser.parse(s).iterator();
	    };
	    data = (MRData)result.next();
	    return true;
	}

        public MRContainer getCurrentKey () throws IOException {
	    return new MRContainer(new MR_long(fsin.getPos()));
	}

	public MRContainer getCurrentValue () throws IOException {
	    return new MRContainer(data);
	}

	public long getPos () throws IOException { return fsin.getPos(); }

	public void close () throws IOException { fsin.close(); }

	public float getProgress () throws IOException {
	    if (end == start)
		return 0.0f;
	    else return Math.min(1.0f, (getPos() - start) / (float)(end - start));
	}

	public void initialize ( InputSplit split, TaskAttemptContext context ) throws IOException { }
    }

    public RecordReader<MRContainer,MRContainer>
	      createRecordReader ( InputSplit split,
				   TaskAttemptContext context ) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	String path = ((FileSplit)split).getPath().toString();
	ParsedDataSource ds = (ParsedDataSource)DataSource.get(path,conf);
	return new ParsedRecordReader((FileSplit)split,context,ds.parser,(Trees)ds.args);
    }

    Bag materialize ( final Path path ) throws IOException {
	Configuration conf = Plan.conf;
	ParsedDataSource ds = (ParsedDataSource)DataSource.get(path.toString(),conf);
	FileSystem fs = path.getFileSystem(conf);
	FSDataInputStream fsin = fs.open(path);
	Parser p;
	try {
	    p = ds.parser.newInstance();
	} catch (Exception ex) {
	    throw new Error("Unrecognized parser:"+ds.parser);
	};
	final Parser parser = p;
	parser.initialize(ds.args);
	parser.open(fsin,0,Long.MAX_VALUE);
	return new Bag(new BagIterator () {
		Iterator<MRData> iter;
		public boolean hasNext () {
		    while (iter == null || !iter.hasNext()) {
			String line = parser.slice();
			if (line == null)
			    return false;
			iter = parser.parse(line).iterator();
		    };
		    return true;
		}
		public MRData next () {
		    return iter.next();
		}
	    });
    }
}


final class GeneratorInputFormat extends MRQLFileInputFormat {
    public static class GeneratorRecordReader extends RecordReader<MRContainer,MRContainer> {
	final long offset;
	final long size;
	long index;
	SequenceFile.Reader reader;

	public GeneratorRecordReader ( FileSplit split,
				       TaskAttemptContext context ) throws IOException {
	    Configuration conf = context.getConfiguration();
	    Path path = split.getPath();
	    FileSystem fs = path.getFileSystem(conf);
	    reader = new SequenceFile.Reader(path.getFileSystem(conf),path,conf);
	    MRContainer key = new MRContainer();
	    MRContainer value = new MRContainer();
	    reader.next(key,value);
	    offset = ((MR_long)((Tuple)(value.data())).first()).get();
	    size = ((MR_long)((Tuple)(value.data())).second()).get();
	    index = -1;
	}

	public boolean nextKeyValue () throws IOException {
	    index++;
	    return index < size;
	}

        public MRContainer getCurrentKey () throws IOException {
	    return new MRContainer(new MR_long(index));
	}

	public MRContainer getCurrentValue () throws IOException {
	    return new MRContainer(new MR_long(offset+index));
	}

	public void close () throws IOException { reader.close(); }

	public float getProgress () throws IOException {
	    return index / (float)size;
	}

	public void initialize ( InputSplit split, TaskAttemptContext context ) throws IOException { }
    }

    public RecordReader<MRContainer,MRContainer>
	      createRecordReader ( InputSplit split, TaskAttemptContext context ) throws IOException {
	return new GeneratorRecordReader((FileSplit)split,context);
    }

    Bag materialize ( final Path path ) throws IOException {
	Configuration conf = Plan.conf;
	FileSystem fs = path.getFileSystem(conf);
	final SequenceFile.Reader reader = new SequenceFile.Reader(path.getFileSystem(conf),path,conf);
	final MRContainer key = new MRContainer();
	final MRContainer value = new MRContainer();
	return new Bag(new BagIterator () {
		long offset = 0;
		long size = 0;
		long i = 0;
		public boolean hasNext () {
		    if (++i >= offset+size)
			try {
			    if (!reader.next(key,value))
				return false;
			    offset = ((MR_long)((Tuple)(value.data())).first()).get();
			    size = ((MR_long)((Tuple)(value.data())).second()).get();
			    i = offset;
			} catch (IOException e) {
			    throw new Error("Cannot collect values from a generator");
			};
		    return true;
		}
		public MRData next () {
		    return new MR_long(i);
		}
	    });
    }
}
