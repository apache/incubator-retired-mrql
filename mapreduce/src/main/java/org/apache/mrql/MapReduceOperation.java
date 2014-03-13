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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


/** The MapReduce operation that uses an in-mapper combiner to partially reduce groups during mapping */
final public class MapReduceOperation extends MapReducePlan {

    public final static class MRContainerPartitioner extends Partitioner<MRContainer,MRContainer> {
        final public int getPartition ( MRContainer key, MRContainer value, int numPartitions ) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    /** The mapper of the MapReduce operation */
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
                } else {
                    // in-mapper combiner
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
                            hashTable.put(p.first(),x);  // normally, done once
                    }
                }
            }
        }

        private static void flush_table ( Context context ) throws IOException, InterruptedException {
            Enumeration<MRData> en = hashTable.keys();
            while (en.hasMoreElements()) {
                MRData key = en.nextElement();
                ckey.set(key);
                MRData value = hashTable.get(key);
                cvalue.set(value);
                if (value != null)
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
            if (hashTable != null)
                flush_table(context);
            hashTable = null; // garbage-collect it
            super.cleanup(context);
        }
    }

    /** The reducer of the MapReduce operation */
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
                streamed = PlanGeneration.streamed_MapReduce_reducer(code);
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

    /**
     * The MapReduce physical operator
     * @param map_fnc          the mapper function
     * @param combine_fnc      optional in-mapper combiner function
     * @param reduce_fnc       the reducer function
     * @param acc_fnc          optional accumulator function
     * @param zero             optional the zero value for the accumulator
     * @param source           the input data source
     * @param num_reduces      number of reducers
     * @param stop_counter     optional counter used in repeat operation
     * @param orderp           does the result need to be ordered?
     * @return a new data source that contains the result
     */
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
        if (combine_fnc != null)
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
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,MRMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(newpath));
        job.setReducerClass(MRReducer.class);
        if (Config.trace && PlanGeneration.streamed_MapReduce_reducer(reduce_fnc))
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
}
