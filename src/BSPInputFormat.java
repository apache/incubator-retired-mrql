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
   The input format used by MRQL/BSP data sources
   Programmer: Leonidas Fegaras, UTA
   Date: 07/06/11 - 08/10/12

********************************************************************************/

package hadoop.mrql;

import Gen.Trees;
import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hama.bsp.*;
import org.apache.hama.HamaConfiguration;


abstract class MRQLFileInputFormat extends FileInputFormat<MRContainer,MRContainer> {
    public MRQLFileInputFormat () {}

    abstract public RecordReader<MRContainer,MRContainer>
	getRecordReader ( InputSplit split, BSPJob job ) throws IOException;

    public final static Bag collect ( final DataSet x ) throws Exception {
	Bag res = new Bag();
	for ( DataSource s: x.source )
	    if (s.to_be_merged)
		res = res.union(Plan.merge(s));
	    else {
		Path path = new Path(s.path);
		final FileSystem fs = path.getFileSystem(Plan.conf);
		final FileStatus[] ds
		    = fs.listStatus(path,
				    new PathFilter () {
			                public boolean accept ( Path path ) {
					    return !path.getName().startsWith("_");
					}
				    });
		for ( FileStatus st: ds )
		    res = res.union(s.inputFormat.newInstance().materialize(st.getPath()));
		final Iterator iter = res.iterator();
		return new Bag(new BagIterator() {
			public boolean hasNext () {
			    return iter.hasNext();
			}
			public MRData next () {
			    // remove source_num
			    return ((Tuple)iter.next()).get(1);
			}
		    });
	    };
	return res;
    }

    private Bag materialize ( final Path file ) throws Exception {
	final BSPJob job = new BSPJob((HamaConfiguration)Plan.conf,MRQLFileInputFormat.class);
	job.setInputPath(file);
	final InputSplit[] splits = getSplits(job,1);
	final RecordReader<MRContainer,MRContainer> rd = getRecordReader(splits[0],job);
	return new Bag(new BagIterator () {
		RecordReader<MRContainer,MRContainer> reader = rd;
		MRContainer key = reader.createKey();
		MRContainer value = reader.createKey();
		int i = 0;
		public boolean hasNext () {
		    try {
			if (reader.next(key,value))
			    return true;
			do {
			    if (++i >= splits.length)
				return false;
			    reader.close();
			    reader = getRecordReader(splits[i],job);
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
    }
}


final class BinaryInputFormat extends MRQLFileInputFormat {
    public static class BinaryInputRecordReader extends SequenceFileRecordReader<MRContainer,MRContainer> {
	final MRContainer result = new MRContainer();
	final MRData source_num_data;
	final int source_number;

	public BinaryInputRecordReader ( FileSplit split,
					 BSPJob job,
					 int source_number ) throws IOException {
	    super(job.getConf(),split);
	    this.source_number = source_number;
	    source_num_data = new MR_int(source_number);
	}

	@Override
	public synchronized boolean next ( MRContainer key, MRContainer value ) throws IOException {
		boolean b = super.next(key,result);
		value.set(new Tuple(source_num_data,result.data()));
		return b;
	}
    }

    public RecordReader<MRContainer,MRContainer>
	      getRecordReader ( InputSplit split,
				BSPJob job ) throws IOException {
	Configuration conf = job.getConf();
	String path = ((FileSplit)split).getPath().toString();
	BinaryDataSource ds = (BinaryDataSource)DataSource.get(path,conf);
	return new BinaryInputRecordReader((FileSplit)split,job,ds.source_num);
    }
}


final class ParsedInputFormat extends MRQLFileInputFormat {
    public static class ParsedRecordReader implements RecordReader<MRContainer,MRContainer> {
	final FSDataInputStream fsin;
	final long start;
	final long end;
	final int source_number;
	final MRData source_num_data;
	Iterator<MRData> result;
	Parser parser;

	public ParsedRecordReader ( FileSplit split,
				    BSPJob job,
				    Class<? extends Parser> parser_class,
				    int source_number,
				    Trees args ) throws IOException {
	    Configuration conf = job.getConf();
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
	    this.source_number = source_number;
	    source_num_data = new MR_int(source_number);
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
	    value.set(new Tuple(source_num_data,(MRData)result.next()));
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

    public RecordReader<MRContainer,MRContainer>
	      getRecordReader ( InputSplit split,
				BSPJob job ) throws IOException {
	Configuration conf = job.getConf();
	String path = ((FileSplit)split).getPath().toString();
	ParsedDataSource ds = (ParsedDataSource)DataSource.get(path,conf);
	return new ParsedRecordReader((FileSplit)split,job,ds.parser,ds.source_num,(Trees)ds.args);
    }
}


final class GeneratorInputFormat extends MRQLFileInputFormat {
    public static class GeneratorRecordReader implements RecordReader<MRContainer,MRContainer> {
	final long offset;
	final long size;
	final int source_number;
	final MRData source_num_data;
	long index;
	SequenceFile.Reader reader;

	public GeneratorRecordReader ( FileSplit split,
				       int source_number,
				       BSPJob job ) throws IOException {
	    Configuration conf = job.getConf();
	    Path path = split.getPath();
	    FileSystem fs = path.getFileSystem(conf);
	    reader = new SequenceFile.Reader(path.getFileSystem(conf),path,conf);
	    MRContainer key = new MRContainer();
	    MRContainer value = new MRContainer();
	    reader.next(key,value);
	    offset = ((MR_long)((Tuple)(value.data())).first()).get();
	    size = ((MR_long)((Tuple)(value.data())).second()).get();
	    this.source_number = source_number;
	    source_num_data = new MR_int(source_number);
	    index = -1;
	}

	public MRContainer createKey () {
	    return new MRContainer(null);
	}

	public MRContainer createValue () {
	    return new MRContainer(null);
	}

	public boolean next ( MRContainer key, MRContainer value ) throws IOException {
	    index++;
	    value.set(new Tuple(source_num_data,new MR_long(offset+index)));
	    key.set(new MR_long(index));
	    return index < size;
	}

	public long getPos () throws IOException { return index; }

	public void close () throws IOException { reader.close(); }

	public float getProgress () throws IOException {
	    return index / (float)size;
	}

	public void initialize ( InputSplit split, TaskAttemptContext context ) throws IOException { }
    }

    public RecordReader<MRContainer,MRContainer>
	        getRecordReader ( InputSplit split, BSPJob job ) throws IOException {
	Configuration conf = job.getConf();
	String path = ((FileSplit)split).getPath().toString();
	GeneratorDataSource ds = (GeneratorDataSource)DataSource.get(path,conf);
	return new GeneratorRecordReader((FileSplit)split,ds.source_num,job);
    }
}


class MultipleBSPInput extends MRQLFileInputFormat {
    public RecordReader<MRContainer,MRContainer>
	      getRecordReader ( InputSplit split, BSPJob job ) throws IOException {
	String path = ((FileSplit)split).getPath().toString();
	Configuration conf = job.getConf();
	DataSource ds = DataSource.get(path,conf);
	Plan.conf = conf;
	if (ds instanceof ParsedDataSource)
	    return new ParsedInputFormat.ParsedRecordReader((FileSplit)split,
							    job,
							    ((ParsedDataSource)ds).parser,
							    ds.source_num,
							    (Trees)((ParsedDataSource)ds).args);
	else if (ds instanceof BinaryDataSource)
	    return new BinaryInputFormat.BinaryInputRecordReader((FileSplit)split,job,ds.source_num);
	else if (ds instanceof GeneratorDataSource)
	    return new GeneratorInputFormat.GeneratorRecordReader((FileSplit)split,ds.source_num,job);
	else throw new Error("Unknown data source: "+ds+" for path "+path);
    }
}
