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

   File: MapReducePlan.java
   The Java physical operators for MRQL
   Programmer: Leonidas Fegaras, UTA
   Date: 10/14/10 - 08/15/12

********************************************************************************/

package hadoop.mrql;

import Gen.*;
import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;


// An anonymous function used by the repeat physical operator
interface DataSetFunction {
    DataSet eval ( final DataSet v );
}


// For reduce-side join, we concatenate 1 or 2 to the join key before we mix the right with the left tuples
final class JoinKey implements Writable {
    public byte tag;     // 1 or 2
    public MRData key;

    JoinKey () {}
    JoinKey  ( byte t, MRData k ) { tag = t; key = k; }

    public void write ( DataOutput out ) throws IOException {
	out.writeByte(tag);
	key.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
	tag = in.readByte();
	key = MRContainer.read(in);
    }

    public String toString () {
	return "<"+tag+":"+key+">";
    }
}


// The MRQL physical operators
class MapReducePlan extends Plan {

    MapReducePlan () {}

    // number of records in the job output
    public final static long outputRecords ( Job job ) throws Exception {
	CounterGroup cg = job.getCounters().getGroup("org.apache.hadoop.mapred.Task$Counter");
	long rc = cg.findCounter("REDUCE_OUTPUT_RECORDS").getValue();
	if (rc == 0)
	    return cg.findCounter("MAP_OUTPUT_RECORDS").getValue();
	return rc;
    }

    public final static class MRContainerPartitioner extends Partitioner<MRContainer,MRContainer> {
	final public int getPartition ( MRContainer key, MRContainer value, int numPartitions ) {
	    return Math.abs(key.hashCode()) % numPartitions;
	}
    }

    public final static class MRContainerJoinPartitioner extends Partitioner<JoinKey,MRContainer> {
	final public int getPartition ( JoinKey key, MRContainer value, int numPartitions ) {
	    return Math.abs(key.key.hashCode()) % numPartitions;
	}
    }

    // sorting: with major order: join key, minor order: tag
    public final static class MRContainerSortComparator implements RawComparator<JoinKey> {
	int[] container_size;

	public MRContainerSortComparator () {
	    container_size = new int[1];
	}

        final public int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl ) {
	    int cmp = MRContainer.compare(x,xs+1,xl-1,y,ys+1,yl-1,container_size);
            return (cmp == 0) ? x[xs]-y[ys] : cmp;
        }

        final public int compare ( JoinKey x, JoinKey y ) {
            int c = x.key.compareTo(y.key);
            return (c == 0) ? x.tag-y.tag : c;
        }
    }

    // grouping: based on join key only
    public final static class MRContainerGroupingComparator implements RawComparator<JoinKey> {
	int[] container_size;

	public MRContainerGroupingComparator() {
	    container_size = new int[1];
	}

        final public int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl ) {
	    return MRContainer.compare(x,xs+1,xl-1,y,ys+1,yl-1,container_size);
	}

        final public int compare ( JoinKey x, JoinKey y ) {
            return x.key.compareTo(y.key);
        }
    }

    // The MapReduce mapper
    private final static class MRMapper extends Mapper<MRContainer,MRContainer,MRContainer,MRContainer> {
	private static Function map_fnc;                    // the map function
	private static Function combine_fnc;                // the combiner function
	private static Hashtable<MRData,MRData> hashTable;  // in-mapper combiner
	private static int index;
	private static Tuple tkey = new Tuple(2);
	private static Bag tbag = new Bag(2);
	private static MRContainer ckey = new MRContainer(new MR_int(0));
	private static MRContainer cvalue = new MRContainer(new MR_int(0));

	@Override
	public void map ( MRContainer key, MRContainer value, Context context )
	            throws IOException, InterruptedException {
	    for ( MRData e: (Bag)map_fnc.eval(value.data()) ) {
		Tuple p = (Tuple)e;
		if (hashTable == null) {
		    ckey.set(p.first());
		    cvalue.set(p.second());
		    context.write(ckey,cvalue);
		} else { // in-mapper combiner
		    MRData old = hashTable.get(p.first());
		    if (old == null) {
			if (index++ == Config.map_cache_size)
			    flush_table(context);
			hashTable.put(p.first(),p.second());
		    } else {
			tkey.set(0,p.first());
			tbag.clear();
			tbag.add_element(p.second()).add_element(old);
			tkey.set(1,tbag);
			for ( MRData x: (Bag)combine_fnc.eval(tkey) )
			    hashTable.put(p.first(),x);
		    }
		}
	    }
	}

	protected static void flush_table ( Context context ) throws IOException, InterruptedException {
	    Enumeration<MRData> en = hashTable.keys();
	    while (en.hasMoreElements()) {
		MRData key = en.nextElement();
		ckey.set(key);
		cvalue.set(hashTable.get(key));
		context.write(ckey,cvalue);
	    };
	    index = 0;
	    hashTable.clear();
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.mapper"));
		map_fnc = functional_argument(conf,code);
		code = Tree.parse(conf.get("mrql.combiner"));
		hashTable = null;
		if (code != null && !code.equals(new VariableLeaf("null"))) {
		    combine_fnc = functional_argument(conf,code);
		    hashTable = new Hashtable<MRData,MRData>(Config.map_cache_size);
		    index = 0;
		}
	    } catch (Exception e) {
		throw new Error("Cannot retrieve the mapper plan");
	    }
	}

	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
	    super.cleanup(context);
	    if (hashTable != null)
		flush_table(context);
	    hashTable = null; // garbage-collect it
	}
    }

    // The MapReduce reducer
    private final static class MRReducer extends Reducer<MRContainer,MRContainer,MRContainer,MRContainer> {
	private static String counter;       // a Hadoop user-defined counter used in the repeat operation
	private static Function reduce_fnc;  // the reduce function
	private static Bag s = new Bag();    // a cached bag of input fragments
	private static Function acc_fnc;     // aggregator
	private static MRData result;        // aggregation result
	private static boolean streamed = false;
	private static Tuple pair = new Tuple(2);
	private static MRContainer container = new MRContainer(new MR_int(0));

	private void write ( MRContainer key, MRData value, Context context )
	             throws IOException, InterruptedException {
	    if (result != null) {  // aggregation
		pair.set(0,result);
		pair.set(1,value);
		result = acc_fnc.eval(pair);
	    } else if (counter.equals("-")) {
		container.set(value);
		context.write(key,container);
	    } else {     // increment the repetition counter if the repeat condition is true
		Tuple t = (Tuple)value;
		if (((MR_bool)t.second()).get())
		    context.getCounter("mrql",counter).increment(1);
		container.set(t.first());
		context.write(key,container);
	    }
	}

	@Override
	public void reduce ( MRContainer key, Iterable<MRContainer> values, Context context )
	            throws IOException, InterruptedException {
	    if (!streamed) {  // store the values in a Bag and then reduce
		s.clear();
		for ( MRContainer val: values )
		    s.add(val.data());
		pair.set(0,key.data());
		pair.set(1,s);
		for ( MRData e: (Bag)reduce_fnc.eval(pair) )
		    write(key,e,context);
	    } else {  // it accesses the values in stream-like fashion
		final Iterator<MRContainer> iterator = values.iterator();
		Bag s = new Bag(new BagIterator() {
			public boolean hasNext () {
			    return iterator.hasNext();
			}
			public MRData next () {
			    return iterator.next().data();
			}
		    });
		pair.set(0,key.data());
		pair.set(1,s);
		for ( MRData e: (Bag)reduce_fnc.eval(pair) )
		    write(key,e,context);
	    }
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.reducer"));
		reduce_fnc = functional_argument(conf,code);
		streamed = Translate.streamed_MapReduce_reducer(code);
		if (conf.get("mrql.zero") != null) {
		    code = Tree.parse(conf.get("mrql.zero"));
		    result = Interpreter.evalE(code);
		    code = Tree.parse(conf.get("mrql.accumulator"));
		    acc_fnc = functional_argument(conf,code);
		} else result = null;
		counter = conf.get("mrql.counter");
	    } catch (Exception e) {
		throw new Error("Cannot retrieve the reduce plan");
	    }
	}

	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
	    if (result != null)  // emit the result of aggregation
		context.write(new MRContainer(new MR_int(0)),new MRContainer(result));
	    super.cleanup(context);
	}
    }

    // The MapReduce physical operator
    public final static DataSet mapReduce ( Tree map_fnc,         // mapper function
					    Tree combine_fnc,     // optional in-mapper combiner function
					    Tree reduce_fnc,      // reducer function
					    Tree acc_fnc,         // optional accumulator function
					    Tree zero,            // optional the zero value for the accumulator
					    DataSet source,       // input data source
					    int num_reduces,      // number of reducers
					    String stop_counter,  // optional counter used in repeat operation
					    boolean orderp )      // does the result need to be ordered?
	                        throws Exception {
	String newpath = new_path(conf);
	conf.set("mrql.mapper",map_fnc.toString());
	conf.set("mrql.combiner",combine_fnc.toString());
	conf.set("mrql.reducer",reduce_fnc.toString());
	if (zero != null) {   // will use in-mapper combiner
	    conf.set("mrql.accumulator",acc_fnc.toString());
	    conf.set("mrql.zero",zero.toString());
	    // the in-mapper combiner likes large data splits
	    conf.set("mapred.min.split.size","268435456");   // 256 MBs
	} else conf.set("mrql.zero","");
	conf.set("mrql.counter",stop_counter);
	Job job = new Job(conf,newpath);
	distribute_compiled_arguments(job.getConfiguration());
	job.setJarByClass(MapReducePlan.class);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setPartitionerClass(MRContainerPartitioner.class);
	job.setSortComparatorClass(MRContainerKeyComparator.class);
	job.setGroupingComparatorClass(MRContainerKeyComparator.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	for (DataSource p: source.source)
	    MultipleInputs.addInputPath(job,new Path(p.path),p.inputFormat,MRMapper.class);
	FileOutputFormat.setOutputPath(job,new Path(newpath));
	job.setReducerClass(MRReducer.class);
	if (Config.trace && Translate.streamed_MapReduce_reducer(reduce_fnc))
	    System.err.println("*** Streamed MapReduce reducer");
	if (num_reduces > 0)
	    job.setNumReduceTasks(num_reduces);
	job.waitForCompletion(true);
	long c = (stop_counter.equals("-")) ? 0
	         : job.getCounters().findCounter("mrql",stop_counter).getValue();
	DataSource s = new BinaryDataSource(newpath,conf);
	s.to_be_merged = orderp;
	return new DataSet(s,c,outputRecords(job));
    }

    // The Map mapper
    private final static class cMapMapper extends Mapper<MRContainer,MRContainer,MRContainer,MRContainer> {
	private static String counter;       // a Hadoop user-defined counter used in the repeat operation
	private static Function map_fnc;     // the mapper function
	private static Function acc_fnc;     // aggregator
	private static MRData result;        // aggregation result
	private static Tuple pair = new Tuple(2);
	private static MRContainer container = new MRContainer(new MR_int(0));

	private void write ( MRContainer key, MRData value, Context context )
	             throws IOException, InterruptedException {
	    if (result != null) {  // aggregation
		pair.set(0,result);
		pair.set(1,value);
		result = acc_fnc.eval(pair);
	    } else if (counter.equals("-")) {
		container.set(value);
		context.write(key,container);
	    } else {     // increment the repetition counter if the repeat condition is true
		Tuple t = (Tuple)value;
		if (((MR_bool)t.second()).get())
		    context.getCounter("mrql",counter).increment(1);
		container.set(t.first());
		context.write(key,container);
	    }
	}

	@Override
	public void map ( MRContainer key, MRContainer value, Context context )
	            throws IOException, InterruptedException {
	    for ( MRData e: (Bag)map_fnc.eval(value.data()) )
		write(key,e,context);
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.mapper"));
		map_fnc = functional_argument(conf,code);
		if (conf.get("mrql.zero") != null) {
		    code = Tree.parse(conf.get("mrql.zero"));
		    result = Interpreter.evalE(code);
		    code = Tree.parse(conf.get("mrql.accumulator"));
		    acc_fnc = functional_argument(conf,code);
		} else result = null;
		counter = conf.get("mrql.counter");
	    } catch (Exception e) {
		throw new Error("Cannot retrieve the mapper plan");
	    }
	}

	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
	    if (result != null)  // emit the result of aggregation
		context.write(new MRContainer(new MR_int(0)),new MRContainer(result));
	    super.cleanup(context);
	}
    }

    // The cMap physical operator
    public final static DataSet cMap ( Tree map_fnc,         // mapper function
				       Tree acc_fnc,         // optional accumulator function
				       Tree zero,            // optional the zero value for the accumulator
				       DataSet source,       // input data source
				       String stop_counter ) // optional counter used in repeat operation
	                        throws Exception {
	String newpath = new_path(conf);
	conf.set("mrql.mapper",map_fnc.toString());
	conf.set("mrql.counter",stop_counter);
	if (zero != null) {
	    conf.set("mrql.accumulator",acc_fnc.toString());
	    conf.set("mrql.zero",zero.toString());
	    conf.set("mapred.min.split.size","268435456");
	} else conf.set("mrql.zero","");
	Job job = new Job(conf,newpath);
	distribute_compiled_arguments(job.getConfiguration());
	job.setJarByClass(MapReducePlan.class);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	for (DataSource p: source.source)
	    MultipleInputs.addInputPath(job,new Path(p.path),p.inputFormat,cMapMapper.class);
	FileOutputFormat.setOutputPath(job,new Path(newpath));
	job.setNumReduceTasks(0);
	job.waitForCompletion(true);
	long c = (stop_counter.equals("-")) ? 0
	         : job.getCounters().findCounter("mrql",stop_counter).getValue();
	return new DataSet(new BinaryDataSource(newpath,conf),c,outputRecords(job));
    }

    // The left mapper for MapReduce2
    private final static class MapperLeft extends Mapper<MRContainer,MRContainer,JoinKey,MRContainer> {
	private static Function mx;     // the left mapper function
	private static JoinKey join_key = new JoinKey((byte)2,new MR_int(0));
	private static Tuple tvalue = (new Tuple(2)).set(0,new MR_byte(2));
	private static MRContainer cvalue = new MRContainer(tvalue);

	@Override
	public void map ( MRContainer key, MRContainer value, Context context )
	            throws IOException, InterruptedException {
	    for ( MRData e: (Bag)mx.eval(value.data()) ) {
		Tuple p = (Tuple)e;
		join_key.key = p.first();
		tvalue.set(1,p.second());
		cvalue.set(tvalue);
		context.write(join_key,cvalue);
	    }
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.mapper.left"));
		mx = functional_argument(conf,code);
	    } catch (Exception e) {
		throw new Error("Cannot retrieve the left mapper plan");
	    }
	}
    }

    // The right mapper for MapReduce2
    private final static class MapperRight extends Mapper<MRContainer,MRContainer,JoinKey,MRContainer> {
	private static Function my;     // the right mapper function
	private static JoinKey join_key = new JoinKey((byte)1,new MR_int(0));
	private static Tuple tvalue = (new Tuple(2)).set(0,new MR_byte(1));
	private static MRContainer cvalue = new MRContainer(tvalue);

	@Override
	public void map ( MRContainer key, MRContainer value, Context context )
	            throws IOException, InterruptedException {
	    for ( MRData e: (Bag)my.eval(value.data()) ) {
		Tuple p = (Tuple)e;
		join_key.key = p.first();
		tvalue.set(1,p.second());
		cvalue.set(tvalue);
		context.write(join_key,cvalue);
	    }
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.mapper.right"));
		my = functional_argument(conf,code);
	    } catch (Exception e) {
		throw new Error("Cannot retrieve the right mapper plan");
	    }
	}
    }

    // The reducer for MapReduce2
    private static class JoinReducer extends Reducer<JoinKey,MRContainer,MRContainer,MRContainer> {
	private static String counter;        // a Hadoop user-defined counter used in the repeat operation
	private static Function reduce_fnc;   // the reduce function
	private static Bag left = new Bag();  // a cached bag of input fragments from left input
	private static Bag right = new Bag(); // a cached bag of input fragments from right input
	private static Function acc_fnc;      // aggregator
	private static MRData result;         // aggregation result
	private static boolean streamed = false;
	private static Tuple pair = new Tuple(2);
	private static MRContainer ckey = new MRContainer(new MR_int(0));
	private static MRContainer container = new MRContainer(new MR_int(0));

	private void write ( MRContainer key, MRData value, Context context )
	             throws IOException, InterruptedException {
	    if (result != null) {  // aggregation
		pair.set(0,result);
		pair.set(1,value);
		result = acc_fnc.eval(pair);
	    } else if (counter.equals("-")) {
		container.set(value);
		context.write(key,container);
	    } else {     // increment the repetition counter if the repeat condition is true
		Tuple t = (Tuple)value;
		if (((MR_bool)t.second()).get())
		    context.getCounter("mrql",counter).increment(1);
		container.set(t.first());
		context.write(key,container);
	    }
	}

	@Override
	public void reduce ( JoinKey key, Iterable<MRContainer> values, Context context )
	            throws IOException, InterruptedException {
	    if (!streamed) {
		left.clear();
		right.clear();
		for ( MRContainer val: values ) {
		    Tuple p = (Tuple)val.data();
		    if (((MR_byte)p.first()).get() == 1)
			right.add(p.second());
		    else left.add(p.second());
		};
	    } else {   // the left input is processed lazily (as a stream-based bag)
		right.clear();
		Tuple p = null;
		final Iterator<MRContainer> i = values.iterator();
		while (i.hasNext()) {
		    p = (Tuple)i.next().data();
		    if (((MR_byte)p.first()).get() == 2)
			break;
		    right.add(p.second());
		    p = null;
		};
		final Tuple data = p;
		left = new Bag(new BagIterator () {
			boolean first_time = data != null;
			public boolean hasNext () {
			    return first_time || i.hasNext();
			}
			public MRData next () {
			    if (!first_time) {
				Tuple t = (Tuple)i.next().data();
				assert(((MR_byte)(t.first())).get() == 2);
				return t.second();
			    };
			    first_time = false;
			    return data.second();
			}
		    });
	    };
	    pair.set(0,left);
	    pair.set(1,right);
	    for ( MRData e: (Bag)reduce_fnc.eval(pair) ) {
		ckey.set(key.key);
		write(ckey,e,context);
	    }
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.reducer"));
		reduce_fnc = functional_argument(conf,code);
		streamed = Translate.streamed_MapReduce2_reducer(code);
		if (conf.get("mrql.zero") != null) {
		    code = Tree.parse(conf.get("mrql.zero"));
		    result = Interpreter.evalE(code);
		    code = Tree.parse(conf.get("mrql.accumulator"));
		    acc_fnc = functional_argument(conf,code);
		} else result = null;
		counter = conf.get("mrql.counter");
	    } catch (Exception e) {
		throw new Error("Cannot retrieve the reducer plan");
	    }
	}

	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
	    if (result != null)  // emit the result of aggregation
		context.write(new MRContainer(new MR_int(0)),new MRContainer(result));
	    super.cleanup(context);
	}
    }

    // The MapReduce2 physical operator
    public final static DataSet mapReduce2 ( Tree mx,              // left mapper function
					     Tree my,              // right mapper function
					     Tree reduce_fnc,      // reducer function
					     Tree acc_fnc,         // optional accumulator function
					     Tree zero,            // optional the zero value for the accumulator
					     DataSet X,            // left data set
					     DataSet Y,            // right data set
					     int num_reduces,      // number of reducers
					     String stop_counter,  // optional counter used in repeat operation
					     boolean orderp )      // does the result need to be ordered?
                                throws Exception {
	String newpath = new_path(conf);
	conf.set("mrql.mapper.left",mx.toString());
	conf.set("mrql.mapper.right",my.toString());
	conf.set("mrql.reducer",reduce_fnc.toString());
	if (zero != null) {
	    conf.set("mrql.accumulator",acc_fnc.toString());
	    conf.set("mrql.zero",zero.toString());
	    conf.set("mapred.min.split.size","268435456");
	} else conf.set("mrql.zero","");
	conf.set("mrql.counter",stop_counter);
	Job job = new Job(conf,newpath);
	distribute_compiled_arguments(job.getConfiguration());
	job.setMapOutputKeyClass(JoinKey.class);
	job.setJarByClass(MapReducePlan.class);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setPartitionerClass(MRContainerJoinPartitioner.class);
	job.setSortComparatorClass(MRContainerSortComparator.class);
	job.setGroupingComparatorClass(MRContainerGroupingComparator.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	FileOutputFormat.setOutputPath(job,new Path(newpath));
	for (DataSource p: X.source)
	    MultipleInputs.addInputPath(job,new Path(p.path),p.inputFormat,MapperLeft.class);
	for (DataSource p: Y.source)
	    MultipleInputs.addInputPath(job,new Path(p.path),p.inputFormat,MapperRight.class);
	if (Config.trace && Translate.streamed_MapReduce2_reducer(reduce_fnc))
	    System.err.println("*** Streamed MapReduce2 reducer");
	job.setReducerClass(JoinReducer.class);
	if (num_reduces > 0)
	    job.setNumReduceTasks(num_reduces);
	job.waitForCompletion(true);
	long c = (stop_counter.equals("-")) ? 0
	         : job.getCounters().findCounter("mrql",stop_counter).getValue();
	DataSource s = new BinaryDataSource(newpath,conf);
	s.to_be_merged = orderp;
	return new DataSet(s,c,outputRecords(job));
    }

    // The mapper for a fragment-replicate join (map-side join)
    private final static class mapJoinMapper extends Mapper<MRContainer,MRContainer,MRContainer,MRContainer> {
	private static String counter;       // a Hadoop user-defined counter used in the repeat operation
	private static Function reduce_fnc;  // the reduce function
	private static Function probe_map_fnc;
	private static Hashtable<MRData,Bag> built_table;
	private static Function acc_fnc;     // aggregator
	private static MRData result;        // aggregation result
	private static Tuple pair = new Tuple(2);
	private static MRContainer container = new MRContainer(new MR_int(0));
	private static Bag empty_bag = new Bag();
	private static boolean mapJoinReduce = false;

	private void write ( MRContainer key, MRData value, Context context )
	             throws IOException, InterruptedException {
	    if (result != null) {  // aggregation
		pair.set(0,result);
		pair.set(1,value);
		result = acc_fnc.eval(pair);
	    } else if (counter.equals("-")) {
		container.set(value);
		context.write(key,container);
	    } else {     // increment the repetition counter if the repeat condition is true
		Tuple t = (Tuple)value;
		if (((MR_bool)t.second()).get())
		    context.getCounter("mrql",counter).increment(1);
		container.set(t.first());
		context.write(key,container);
	    }
	}

	@Override
	public void map ( MRContainer key, MRContainer value, Context context )
	            throws IOException, InterruptedException {
	    for ( MRData e: (Bag)probe_map_fnc.eval(value.data()) ) {
		Tuple p = (Tuple)e;
		MRData pd = built_table.get(p.first());
		if (pd == null)
		    pd = empty_bag;
		pair.set(0,p.second());
		pair.set(1,pd);
		for ( MRData v: (Bag)reduce_fnc.eval(pair) )
		    write(key,v,context);
	    }
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		if (conf.get("mrql.mapJoinReduce") != null)
		    mapJoinReduce = true;
		Tree code = Tree.parse(conf.get("mrql.inMap.reducer"));
		reduce_fnc = functional_argument(conf,code);
		code = Tree.parse(conf.get("mrql.probe_mapper"));
		probe_map_fnc = functional_argument(conf,code);
		if (!mapJoinReduce && conf.get("mrql.zero") != null) {
		    code = Tree.parse(conf.get("mrql.zero"));
		    result = Interpreter.evalE(code);
		    code = Tree.parse(conf.get("mrql.accumulator"));
		    acc_fnc = functional_argument(conf,code);
		} else result = null;
		counter = conf.get("mrql.counter");
		built_table = new Hashtable<MRData,Bag>(Config.map_cache_size);
		Bag res = new Bag();
		URI[] uris = DistributedCache.getCacheFiles(conf);
		Path[] local_paths = DistributedCache.getLocalCacheFiles(conf);
		final FileSystem fs = FileSystem.getLocal(conf);
		final Configuration fconf = conf;
		for ( int i = 0; i < local_paths.length; i++ ) {
		    // hadoop 0.20.2 distributed cache doesn't work in stand-alone
		    final Path path = (conf.get("mapred.job.tracker").equals("local"))
				      ? new Path(uris[i].toString())
				      : local_paths[i];
		    if (path.getName().endsWith(".jar"))
			continue;
		    res = res.union(new Bag(new BagIterator () {
			    final SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,fconf);
			    final MRContainer key = new MRContainer(new MR_int(0));
			    final MRContainer value = new MRContainer(new MR_int(0));
			    public boolean hasNext () {
				try {
				    boolean done = reader.next(key,value);
				    if (!done)
					reader.close();
				    return done;
				} catch (IOException e) {
				    throw new Error("Cannot collect values from distributed cache");
				}
			    }
			    public MRData next () {
				return value.data();
			    }
			 }));
		};
		for ( MRData e: res ) {
		    Tuple p = (Tuple)e;
		    Bag entries = built_table.get(p.first());
		    built_table.put(p.first(),
				    (entries == null)
				    ? (new Bag(p.second()))
				    : entries.add_element(p.second()));
		}
	    } catch (Exception e) {
		throw new Error("Cannot setup the mapJoin: "+e);
	    }
	}

	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
	    if (result != null)  // emit the result of aggregation
		context.write(new MRContainer(new MR_int(0)),new MRContainer(result));
	    built_table = null; // garbage-collect it
	    super.cleanup(context);
	}
    }

    // The fragment-replicate join (map-side join) physical operator
    public final static DataSet mapJoin ( Tree probe_map_fnc,    // left mapper function
					  Tree built_map_fnc,    // right mapper function
					  Tree reduce_fnc,       // reducer function
					  Tree acc_fnc,          // optional accumulator function
					  Tree zero,             // optional the zero value for the accumulator
					  DataSet probe_dataset, // the map source
					  DataSet built_dataset, // stored in distributed cache
					  String stop_counter )  // optional counter used in repeat operation
                                throws Exception {
	DataSet ds = cMap(built_map_fnc,null,null,built_dataset,"-");
	String newpath = new_path(conf);
	conf.set("mrql.inMap.reducer",reduce_fnc.toString());
	conf.set("mrql.probe_mapper",probe_map_fnc.toString());
	conf.set("mrql.counter",stop_counter);
	if (zero != null) {
	    conf.set("mrql.accumulator",acc_fnc.toString());
	    conf.set("mrql.zero",zero.toString());
	    conf.set("mapred.min.split.size","268435456");
	} else conf.set("mrql.zero","");
	Job job = new Job(conf,newpath);
	distribute_compiled_arguments(job.getConfiguration());
	job.setJarByClass(MapReducePlan.class);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	PathFilter pf = new PathFilter () { public boolean accept ( Path path ) {
	                                        return !path.getName().startsWith("_");
	                                    } };
	for (DataSource p: ds.source) {  // distribute the built dataset
	    Path path = new Path(p.path);
	    for ( FileStatus s: path.getFileSystem(conf).listStatus(path,pf) )
		DistributedCache.addCacheFile(s.getPath().toUri(),job.getConfiguration());
	};
	for (DataSource p: probe_dataset.source)
	    MultipleInputs.addInputPath(job,new Path(p.path),p.inputFormat,mapJoinMapper.class);
	FileOutputFormat.setOutputPath(job,new Path(newpath));
	job.setNumReduceTasks(0);
	job.waitForCompletion(true);
	long c = (stop_counter.equals("-")) ? 0
	         : job.getCounters().findCounter("mrql",stop_counter).getValue();
	return new DataSet(new BinaryDataSource(newpath,conf),c,outputRecords(job));
    }

    // The mapper for the CrossProduct
    private final static class crossProductMapper extends Mapper<MRContainer,MRContainer,MRContainer,MRContainer> {
	private static String counter;       // a Hadoop user-defined counter used in the repeat operation
	private static Function reduce_fnc;  // the reduce function
	private static Function map_fnc;     // the mapper function
	private static DataSet cached_dataset;
	private final static List<MRData> outer
	    = new Vector<MRData>(Config.map_cache_size);  // fix-size cache for the outer
	private static int index;
	private static MRContainer last_key;
	private static URI[] uris;
	private static Path[] local_paths;
	private static Function acc_fnc;     // aggregator
	private static MRData result;        // aggregation result
	private static Tuple pair = new Tuple(2);
	private static MRContainer container = new MRContainer(new MR_int(0));

	private void write ( MRContainer key, MRData value, Context context )
	             throws IOException, InterruptedException {
	    if (result != null) {  // aggregation
		pair.set(0,result);
		pair.set(1,value);
		result = acc_fnc.eval(pair);
	    } else if (counter.equals("-")) {
		container.set(value);
		context.write(key,container);
	    } else {     // increment the repetition counter if the repeat condition is true
		Tuple t = (Tuple)value;
		if (((MR_bool)t.second()).get())
		    context.getCounter("mrql",counter).increment(1);
		container.set(t.first());
		context.write(key,container);
	    }
	}

	@Override
	public void map ( MRContainer key, MRContainer value, Context context )
	            throws IOException, InterruptedException {
	    try {
		last_key = key;
		for ( MRData x: (Bag)map_fnc.eval(value.data()) )
		    if (index++ == Config.map_cache_size) {
			for ( MRData y: cached_data(context.getConfiguration()) ) {
			    pair.set(1,y);
			    for ( MRData z: outer ) {
				pair.set(0,z);
				for ( MRData v: (Bag)reduce_fnc.eval(pair) )
				    write(key,v,context);
			    }
			};
			index = 0;
			outer.clear();
		    } else outer.add(x);
	    } catch (Exception e) {
		throw new Error("Cannot perform the crossProduct: "+e);
	    }
	}

	protected Bag cached_data ( final Configuration conf ) {
	    try {
		Bag res = new Bag();
		final FileSystem fs = FileSystem.getLocal(conf);
		for ( int i = 0; i < local_paths.length; i++ ) {
		    // hadoop 0.20.2 distributed cache doesn't work in stand-alone
		    final Path path = (conf.get("mapred.job.tracker").equals("local"))
			              ? new Path(uris[i].toString())
			              : local_paths[i];
		    if (path.getName().endsWith(".jar"))
			continue;
		    res = res.union(new Bag(new BagIterator () {
			    final SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
			    final MRContainer key = new MRContainer(new MR_int(0));
			    final MRContainer value = new MRContainer(new MR_int(0));
			    public boolean hasNext () {
				try {
				    boolean done = reader.next(key,value);
				    if (!done)
					reader.close();
				    return done;
				} catch (IOException e) {
				    throw new Error("Cannot collect values from distributed cache");
				}
			    }
			    public MRData next () {
				return value.data();
			    }
			}));
		};
		return res;
	    } catch (Exception e) {
		throw new Error("Cannot setup the cross product: "+e);
	    }
	}

	@Override
	protected void setup ( Context context ) throws IOException,InterruptedException {
	    super.setup(context);
	    try {
		Configuration conf = context.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		Tree code = Tree.parse(conf.get("mrql.reducer"));
		reduce_fnc = functional_argument(conf,code);
		code = Tree.parse(conf.get("mrql.mapper"));
		map_fnc = functional_argument(conf,code);
		if (conf.get("mrql.zero") != null) {
		    code = Tree.parse(conf.get("mrql.zero"));
		    result = Interpreter.evalE(code);
		    code = Tree.parse(conf.get("mrql.accumulator"));
		    acc_fnc = functional_argument(conf,code);
		} else result = null;
		counter = conf.get("mrql.counter");
		uris = DistributedCache.getCacheFiles(conf);
		local_paths = DistributedCache.getLocalCacheFiles(conf);
		index = 0;
	    } catch (Exception e) {
		throw new Error("Cannot setup the crossProduct: "+e);
	    }
	}

	@Override
	protected void cleanup ( Context context ) throws IOException,InterruptedException {
	    if (index > 0)
		try {
		    for ( MRData y: cached_data(context.getConfiguration()) ) {
			pair.set(1,y);
			for ( MRData z: outer ) {
			    pair.set(0,z);
			    for ( MRData v: (Bag)reduce_fnc.eval(pair) )
				write(last_key,v,context);
			}
		    };
		} catch (Exception e) {
		    throw new Error("Cannot cleanup the crossProduct: "+e);
		};
	    index = 0;
	    outer.clear();
	    if (result != null)  // emit the result of aggregation
		context.write(new MRContainer(new MR_int(0)),new MRContainer(result));
	    super.cleanup(context);
	}
    }

    // The CrossProduct physical operator (similar to block-nested loop)
    public final static DataSet crossProduct ( Tree mx,              // left mapper
					       Tree my,              // right mapper
					       Tree reduce_fnc,      // reducer
					       Tree acc_fnc,         // optional accumulator function
					       Tree zero,            // optional the zero value for the accumulator
					       DataSet X,            // the left source
					       DataSet Y,            // the right source (stored in distributed cache)
					       String stop_counter ) // optional counter used in repeat operation
                                 throws Exception {
	DataSet ds = cMap(my,null,null,Y,"-");
	String newpath = new_path(conf);
	conf.set("mrql.reducer",reduce_fnc.toString());
	conf.set("mrql.mapper",mx.toString());
	if (zero != null) {
	    conf.set("mrql.accumulator",acc_fnc.toString());
	    conf.set("mrql.zero",zero.toString());
	    conf.set("mapred.min.split.size","268435456");
	} else conf.set("mrql.zero","");
	conf.set("mrql.counter",stop_counter);
	Job job = new Job(conf,newpath);
	distribute_compiled_arguments(job.getConfiguration());
	job.setJarByClass(MapReducePlan.class);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	PathFilter pf = new PathFilter () { public boolean accept ( Path path ) {
	                                        return !path.getName().startsWith("_");
	                                    } };
	for (DataSource p: ds.source) {
	    Path path = new Path(p.path);
	    for ( FileStatus s: path.getFileSystem(conf).listStatus(path,pf) )
		DistributedCache.addCacheFile(s.getPath().toUri(),job.getConfiguration());
	};
	for (DataSource p: X.source)
	    MultipleInputs.addInputPath(job,new Path(p.path),p.inputFormat,crossProductMapper.class);
	FileOutputFormat.setOutputPath(job,new Path(newpath));
	job.setNumReduceTasks(0);
	job.waitForCompletion(true);
	long c = (stop_counter.equals("-")) ? 0
	         : job.getCounters().findCounter("mrql",stop_counter).getValue();
	return new DataSet(new BinaryDataSource(newpath,conf),c,outputRecords(job));
    }

    // The Aggregate physical operator
    public final static MRData aggregate ( final Tree acc_fnc,
					   final Tree zero,
					   final DataSet x ) throws Exception {
	MRData res = Interpreter.evalE(zero);
	Function accumulator = functional_argument(null,acc_fnc);
	Tuple pair = new Tuple(2);
	for ( DataSource s: x.source )
	    if (s.inputFormat != BinaryInputFormat.class) {
		pair.set(0,res);
		pair.set(1,aggregate(acc_fnc,zero,
				     cMap(Interpreter.identity_mapper,acc_fnc,zero,
					  new DataSet(s,0,0),"-")));
		res = accumulator.eval(pair);
	    } else {
		Path path = new Path(s.path);
		final FileSystem fs = path.getFileSystem(conf);
		final FileStatus[] ds
		    = fs.listStatus(path,
				    new PathFilter () {
			                public boolean accept ( Path path ) {
					    return !path.getName().startsWith("_");
					}
				    });
		MRContainer key = new MRContainer(new MR_int(0));
		MRContainer value = new MRContainer(new MR_int(0));
		for ( int i = 0; i < ds.length; i++ ) {
		    SequenceFile.Reader reader = new SequenceFile.Reader(fs,ds[i].getPath(),conf);
		    while (reader.next(key,value)) {
			pair.set(0,res);
			pair.set(1,value.data());
			res = accumulator.eval(pair);
		    };
		    reader.close();
		}
	    };
	return res;
    }

    // The repeat physical operator
    public final static DataSet repeat ( Function loop, DataSet init, int max_num ) {
	MR_dataset s = new MR_dataset(init);
	int i = 0;
	do {
	    s.dataset = ((MR_dataset)loop.eval(s)).dataset;
	    i++;
	    System.err.println("*** Repeat #"+i+": "+s.dataset.counter+" true results");
	} while (s.dataset.counter != 0 && i < max_num);
	return s.dataset;
    }

    // The closure physical operator
    public final static DataSet closure ( Function loop, DataSet init, int max_num ) {
	MR_dataset s = new MR_dataset(init);
	int i = 0;
	long n = 0;
	long old = 0;
	do {
	    s.dataset = ((MR_dataset)loop.eval(s)).dataset;
	    i++;
	    System.err.println("*** Repeat #"+i+": "+(s.dataset.records-n)+" new records");
	    old = n;
	    n = s.dataset.records;
	} while (old < n && i < max_num);
	return s.dataset;
    }
}
