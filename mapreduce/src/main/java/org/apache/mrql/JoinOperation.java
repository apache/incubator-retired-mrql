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
import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Enumeration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


/** The MapReduce2 physical operator (a reduce-side join) */
final public class JoinOperation extends MapReducePlan {

    /** Container for join input values. For reduce-side join, we concatenate
     * 1 or 2 to the join key before we mix the right with the left tuples */
    public final static class JoinKey implements Writable {
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

    public final static class MRContainerJoinPartitioner extends Partitioner<JoinKey,MRContainer> {
        final public int getPartition ( JoinKey key, MRContainer value, int numPartitions ) {
            return Math.abs(key.key.hashCode()) % numPartitions;
        }
    }

    /** The sorting of the joined values uses ths join key for major order and tag for minor order */
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

    /** The grouping of the joined values is based on join key only */
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

    /** The left mapper for MapReduce2 */
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

        @Override
        protected void cleanup ( Context context ) throws IOException,InterruptedException {
            super.cleanup(context);
        }
    }

    /** The right mapper for MapReduce2 */
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

        @Override
        protected void cleanup ( Context context ) throws IOException,InterruptedException {
            super.cleanup(context);
        }
    }

    /** The reducer for MapReduce2 */
    private static class JoinReducer extends Reducer<JoinKey,MRContainer,MRContainer,MRContainer> {
        private static String counter;        // a Hadoop user-defined counter used in the repeat operation
        private static Function combine_fnc;  // the combiner function
        private static Function reduce_fnc;   // the reduce function
        private static Bag left = new Bag();  // a cached bag of input fragments from left input
        private static Bag right = new Bag(); // a cached bag of input fragments from right input
        private static Function acc_fnc;      // aggregator
        private static MRData result;         // aggregation result
        private static Hashtable<MRData,MRData> hashTable;  // in-mapper combiner
        private static int index;
        private static Tuple tkey = new Tuple(2);
        private static Bag tbag = new Bag(2);
        private static boolean streamed = false;
        private static Tuple pair = new Tuple(2);
        private static MRContainer ckey = new MRContainer(new MR_int(0));
        private static MRContainer cvalue = new MRContainer(new MR_int(0));
        private static MRContainer container = new MRContainer(new MR_int(0));

        private void write ( MRContainer key, MRData value, Context context )
                     throws IOException, InterruptedException {
            if (result != null) {  // aggregation
                pair.set(0,result);
                pair.set(1,value);
                result = acc_fnc.eval(pair);
            } else if (hashTable != null) {
                MRData k = ((Tuple)value).get(0);
                MRData v = ((Tuple)value).get(1);
                MRData old = hashTable.get(k);
                if (old == null) {
                    if (index++ == Config.map_cache_size)
                        flush_table(context);
                    hashTable.put(k,v);
                } else {
                    tkey.set(0,key.data());
                    tbag.clear();
                    tbag.add_element(v).add_element(old);
                    tkey.set(1,tbag);
                    for ( MRData x: (Bag)combine_fnc.eval(tkey) )
                        hashTable.put(k,x);  // normally, done once
                }
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

        protected static void flush_table ( Context context ) throws IOException, InterruptedException {
            Enumeration<MRData> en = hashTable.keys();
            while (en.hasMoreElements()) {
                MRData key = en.nextElement();
                ckey.set(key);
                MRData value = hashTable.get(key);
                cvalue.set(value);
                if (value != null)
                    context.write(ckey,new MRContainer(new Tuple(key,value)));
            };
            index = 0;
            hashTable.clear();
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
                streamed = PlanGeneration.streamed_MapReduce2_reducer(code);
                if (conf.get("mrql.zero") != null) {
                    code = Tree.parse(conf.get("mrql.zero"));
                    result = Interpreter.evalE(code);
                    code = Tree.parse(conf.get("mrql.accumulator"));
                    acc_fnc = functional_argument(conf,code);
                } else result = null;
                counter = conf.get("mrql.counter");
                code = Tree.parse(conf.get("mrql.combiner"));
                hashTable = null;
                if (code != null && !code.equals(new VariableLeaf("null"))) {
                    combine_fnc = functional_argument(conf,code);
                    hashTable = new Hashtable<MRData,MRData>(Config.map_cache_size);
                    index = 0;
                }
            } catch (Exception e) {
                throw new Error("Cannot retrieve the reducer plan");
            }
        }

        @Override
        protected void cleanup ( Context context ) throws IOException,InterruptedException {
            if (result != null)  // emit the result of aggregation
                context.write(new MRContainer(new MR_int(0)),new MRContainer(result));
            if (hashTable != null)
                flush_table(context);
            hashTable = null; // garbage-collect it
            super.cleanup(context);
        }
    }

    /** The MapReduce2 physical operator (a reduce-side join)
     * @param mx             left mapper function
     * @param my             right mapper function
     * @param combine_fnc    optional in-mapper combiner function
     * @param reduce_fnc     reducer function
     * @param acc_fnc        optional accumulator function
     * @param zero           optional the zero value for the accumulator
     * @param X              left data set
     * @param Y              right data set
     * @param num_reduces    number of reducers
     * @param stop_counter   optional counter used in repeat operation
     * @param orderp         does the result need to be ordered?
     * @return a new data source that contains the result
     */
    public final static DataSet mapReduce2 ( Tree mx,              // left mapper function
                                             Tree my,              // right mapper function
                                             Tree combine_fnc,     // optional in-mapper combiner function
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
        if (combine_fnc != null)
            conf.set("mrql.combiner",combine_fnc.toString());
        conf.set("mrql.reducer",reduce_fnc.toString());
        if (zero != null) {
            conf.set("mrql.accumulator",acc_fnc.toString());
            conf.set("mrql.zero",zero.toString());
            // the in-mapper combiner likes large data splits
            conf.set("mapred.min.split.size","268435456");   // 256 MBs
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
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,MapperLeft.class);
        for (DataSource p: Y.source)
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,MapperRight.class);
        if (Config.trace && PlanGeneration.streamed_MapReduce2_reducer(reduce_fnc))
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
}
