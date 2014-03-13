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
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


/**
 *   A map-reduce job that captures a join with group-by. Similar to matrix multiplication.<br/>
 *   It captures queries of the form:
 * <pre>
 *      select r(kx,ky,c(z))
 *        from x in X, y in Y, z = mp(x,y)
 *       where jx(x) = jy(y)
 *       group by (kx,ky): (gx(x),gy(y));
 * </pre>
 *   where: mp: map function, r: reduce function, c: combine function,
 *          jx: left join key function, jy: right join key function,
 *          gx: left group-by function, gy: right group-by function.
 * <br/>
 *   Example: matrix multiplication:
 * <pre>
 *      select ( sum(z), i, j )
 *        from (x,i,k) in X, (y,k,j) in Y, z = x*y
 *       group by (i,j);
 * </pre>
 *   It uses m*n partitions, so that n/m=|X|/|Y| and a hash table of size |X|/n*|Y|/m can fit in memory M.
 *   That is, n = |X|/sqrt(M), m = |Y|/sqrt(M).
 *   Each partition generates |X|/n*|Y|/m data. It replicates X n times and Y m times.
 *   Uses a hash-table H of size |X|/n*|Y|/m
 *   MapReduce pseudo-code:
 * <pre>
 *   mapX ( x )
 *     for i = 0,n-1
 *        emit ( ((hash(gx(x)) % m)+m*i, jx(x), 1), (1,x) )
 *
 *   mapY ( y )
 *     for i = 0,m-1
 *        emit ( ((hash(gy(y)) % n)*m+i, jy(y), 2), (2,y) )
 * </pre>
 *   mapper output key: (partition,joinkey,tag),  value: (tag,data) <br/>
 *   Partitioner: over partition                                    <br/>
 *   GroupingComparator: over partition and joinkey                 <br/>
 *   SortComparator: major partition, minor joinkey, sub-minor tag  <br/>
 * <pre>
 *   reduce ( (p,_,_), s )
 *     if p != current_partition
 *        flush()
 *        current_partition = p
 *     read x from s first and store it to xs
 *     for each y from the rest of s
 *        for each x in xs
 *            H[(gx(x),gy(y))] = c( H[(gx(x),gy(y))], mp((x,y)) )
 * </pre>
 *   where flush() is: for each ((kx,ky),v) in H: emit r((kx,ky),v)
 */
final public class GroupByJoinPlan extends Plan {

    /** mapper output key: (partition,joinkey,tag) */
    private final static class GroupByJoinKey implements Writable {
        public int partition;  // one of n*m
        public byte tag;       // 1 or 2
        public MRData key;

        GroupByJoinKey () {}
        GroupByJoinKey  ( int p, byte t, MRData k ) {
            partition = p;
            tag = t;
            key = k;
        }

        public void write ( DataOutput out ) throws IOException {
            out.writeByte(tag);
            WritableUtils.writeVInt(out,partition);
            key.write(out);
        }

        public void readFields ( DataInput in ) throws IOException {
            tag = in.readByte();
            partition = WritableUtils.readVInt(in);
            key = MRContainer.read(in);
        }

        public String toString () { return "["+partition+","+tag+","+key+"]"; }
    }

    /** partition based on key.partition only */
    private final static class GroupByJoinPartitioner extends Partitioner<GroupByJoinKey,MRContainer> {
        final public int getPartition ( GroupByJoinKey key, MRContainer value, int numPartitions ) {
            return key.partition % numPartitions;
        }
    }

    /** sorting with major order key.partition, minor key.key, minor key.tag */
    private final static class GroupByJoinSortComparator implements RawComparator<GroupByJoinKey> {
        int[] container_size;

        public GroupByJoinSortComparator () {
            container_size = new int[1];
        }

        final public int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl ) {
            try {
                int c = WritableComparator.readVInt(x,xs+1)-WritableComparator.readVInt(y,ys+1);
                if (c != 0)
                    return c;
                int tsize = 1+WritableUtils.decodeVIntSize(x[xs+1]);
                c = MRContainer.compare(x,xs+tsize,xl-tsize,y,ys+tsize,yl-tsize,container_size);
                if (c != 0)
                    return c;
                return x[xs] - y[ys];
            } catch (IOException e) {
                throw new Error(e);
            }
        }

        final public int compare ( GroupByJoinKey x, GroupByJoinKey y ) {
            int c = x.partition - y.partition;
            if (c != 0)
                return c;
            c = x.key.compareTo(y.key);
            if (c != 0)
                return c;
            return x.tag - y.tag;
        }
    }

    /** grouping by key.partition and key.key */
    private final static class GroupByJoinGroupingComparator implements RawComparator<GroupByJoinKey> {
        int[] container_size;

        public GroupByJoinGroupingComparator() {
            container_size = new int[1];
        }

        final public int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl ) {
            try {
                int c = WritableComparator.readVInt(x,xs+1)-WritableComparator.readVInt(y,ys+1);
                if (c != 0)
                    return c;
                int tsize = 1+WritableUtils.decodeVIntSize(x[xs+1]);
                return MRContainer.compare(x,xs+tsize,xl-tsize,y,ys+tsize,yl-tsize,container_size);
            } catch (IOException e) {
                throw new Error(e);
            }
        }

        final public int compare ( GroupByJoinKey x, GroupByJoinKey y ) {
            int c = x.partition - y.partition;
            return (c != 0) ? c : x.key.compareTo(y.key);
        }
    }

    /** the left GroupByJoin mapper */
    private final static class MapperLeft extends Mapper<MRContainer,MRContainer,GroupByJoinKey,MRContainer> {
        private static int n, m;
        private static Function left_join_key_fnc;
        private static Function left_groupby_fnc;
        private static GroupByJoinKey ckey = new GroupByJoinKey(0,(byte)1,new MR_int(0));
        private static Tuple tvalue = (new Tuple(2)).set(0,new MR_byte(1));
        private static MRContainer cvalue = new MRContainer(tvalue);

        @Override
        public void map ( MRContainer key, MRContainer value, Context context )
                    throws IOException, InterruptedException {
            MRData data = value.data();
            for ( int i = 0; i < n; i++ ) {
                ckey.partition = (left_groupby_fnc.eval(data).hashCode() % m)+m*i;
                ckey.key = left_join_key_fnc.eval(data);
                tvalue.set(1,data);
                context.write(ckey,cvalue);
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
                Tree code = Tree.parse(conf.get("mrql.join.key.left"));
                left_join_key_fnc = functional_argument(conf,code);
                code = Tree.parse(conf.get("mrql.groupby.left"));
                left_groupby_fnc = functional_argument(conf,code);
                m = conf.getInt("mrql.m",1);
                n = conf.getInt("mrql.n",1);
            } catch (Exception e) {
                throw new Error("Cannot retrieve the left mapper plan");
            }
        }
    }

    /** the right GroupByJoin mapper */
    private final static class MapperRight extends Mapper<MRContainer,MRContainer,GroupByJoinKey,MRContainer> {
        private static int n, m;
        private static Function right_join_key_fnc;
        private static Function right_groupby_fnc;
        private static GroupByJoinKey ckey = new GroupByJoinKey(0,(byte)2,new MR_int(0));
        private static Tuple tvalue = (new Tuple(2)).set(0,new MR_byte(2));
        private static MRContainer cvalue = new MRContainer(tvalue);

        @Override
        public void map ( MRContainer key, MRContainer value, Context context )
                    throws IOException, InterruptedException {
            MRData data = value.data();
            for ( int i = 0; i < m; i++ ) {
                ckey.partition = (right_groupby_fnc.eval(data).hashCode() % n)*m+i;
                ckey.key = right_join_key_fnc.eval(data);
                tvalue.set(1,data);
                context.write(ckey,cvalue);
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
                Tree code = Tree.parse(conf.get("mrql.join.key.right"));
                right_join_key_fnc = functional_argument(conf,code);
                code = Tree.parse(conf.get("mrql.groupby.right"));
                right_groupby_fnc = functional_argument(conf,code);
                m = conf.getInt("mrql.m",1);
                n = conf.getInt("mrql.n",1);
            } catch (Exception e) {
                throw new Error("Cannot retrieve the right mapper plan");
            }
        }
    }

    /** the GroupByJoin reducer */
    private static class JoinReducer extends Reducer<GroupByJoinKey,MRContainer,MRContainer,MRContainer> {
        private static String counter;            // a Hadoop user-defined counter used in the repeat operation
        private static int n, m;                  // n*m partitioners
        private static Function left_groupby_fnc; // left group-by function
        private static Function right_groupby_fnc;// right group-by function
        private static Function map_fnc;          // the map function
        private static Function combine_fnc;      // the combine function
        private static Function reduce_fnc;       // the reduce function
        private static Bag left = new Bag();      // a cached bag of input fragments from left input
        private static int current_partition = -1;
        private static Hashtable<MRData,MRData> hashTable;  // in-reducer combiner
        private static Tuple pair = new Tuple(2);
        private static MRContainer ckey = new MRContainer(new MR_int(0));
        private static MRContainer cvalue = new MRContainer(new MR_int(0));
        private static MRContainer container = new MRContainer(new MR_int(0));
        private static Tuple tkey = new Tuple(2);
        private static Bag tbag = new Bag(2);

        private static void write ( MRContainer key, MRData value, Context context )
                       throws IOException, InterruptedException {
            if (counter.equals("-")) {
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

        private void store ( MRData key, MRData value ) throws IOException {
            MRData old = hashTable.get(key);
            Tuple k = (Tuple)key;
            pair.set(0,key);
            for ( MRData e: (Bag)map_fnc.eval(value) )
                if (old == null)
                    hashTable.put(key,e);
                else {
                    tbag.clear();
                    tbag.add_element(e).add_element(old);
                    pair.set(1,tbag);
                    for ( MRData z: (Bag)combine_fnc.eval(pair) )
                        hashTable.put(key,z);  // normally, done once
                }
        }

        protected static void flush_table ( Context context ) throws IOException, InterruptedException {
            Enumeration<MRData> en = hashTable.keys();
            while (en.hasMoreElements()) {
                MRData key = en.nextElement();
                MRData value = hashTable.get(key);
                ckey.set(key);
                pair.set(0,key);
                tbag.clear();
                tbag.add_element(value);
                pair.set(1,tbag);
                for ( MRData e: (Bag)reduce_fnc.eval(pair) )
                    write(ckey,e,context);
            };
            hashTable.clear();
        }

        @Override
        public void reduce ( GroupByJoinKey key, Iterable<MRContainer> values, Context context )
                    throws IOException, InterruptedException {
            if (key.partition != current_partition && current_partition > 0) {
                // at the end of a partition, flush the hash table
                flush_table(context);
                current_partition = key.partition;
            };
            left.clear();
            Tuple p = null;
            final Iterator<MRContainer> i = values.iterator();
            // left tuples arrive before right tuples; cache the left values into the left bag
            while (i.hasNext()) {
                p = (Tuple)i.next().data();
                if (((MR_byte)p.first()).get() == 2)
                    break;
                left.add(p.second());
                p = null;
            };
            // the previous value was from right
            if (p != null) {
                MRData y = p.second();
                MRData gy = right_groupby_fnc.eval(y);
                // cross product with left (must use new Tuples)
                for ( MRData x: left )
                    store(new Tuple(left_groupby_fnc.eval(x),gy),new Tuple(x,y));
                // the rest of values are from right
                while (i.hasNext()) {
                    y = ((Tuple)i.next().data()).second();
                    gy = right_groupby_fnc.eval(y);
                    // cross product with left (must use new Tuples)
                    for ( MRData x: left )
                        store(new Tuple(left_groupby_fnc.eval(x),gy),new Tuple(x,y));
                }
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
                Tree code = Tree.parse(conf.get("mrql.groupby.left"));
                left_groupby_fnc = functional_argument(conf,code);
                code = Tree.parse(conf.get("mrql.groupby.right"));
                right_groupby_fnc = functional_argument(conf,code);
                m = conf.getInt("mrql.m",1);
                n = conf.getInt("mrql.n",1);
                code = Tree.parse(conf.get("mrql.mapper"));
                map_fnc = functional_argument(conf,code);
                code = Tree.parse(conf.get("mrql.combiner"));
                combine_fnc = functional_argument(conf,code);
                code = Tree.parse(conf.get("mrql.reducer"));
                reduce_fnc = functional_argument(conf,code);
                counter = conf.get("mrql.counter");
                hashTable = new Hashtable<MRData,MRData>(Config.map_cache_size);
            } catch (Exception e) {
                throw new Error("Cannot retrieve the reducer plan");
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

    /** the GroupByJoin operation
     * @param left_join_key_fnc   left join key function
     * @param right_join_key_fnc  right join key function
     * @param left_groupby_fnc    left group-by function
     * @param right_groupby_fnc   right group-by function
     * @param map_fnc             map function
     * @param combine_fnc         combine function
     * @param reduce_fnc          reduce function
     * @param X                   left data set
     * @param Y                   right data set
     * @param num_reducers        number of reducers
     * @param n                   left dimension of the reducer grid
     * @param m                   right dimension of the reducer grid
     * @param stop_counter        optional counter used in repeat operation
     * @return a DataSet that contains the result
     */
    public final static DataSet groupByJoin
                 ( Tree left_join_key_fnc,       // left join key function
                   Tree right_join_key_fnc,      // right join key function
                   Tree left_groupby_fnc,        // left group-by function
                   Tree right_groupby_fnc,       // right group-by function
                   Tree map_fnc,                 // map function
                   Tree combine_fnc,             // combine function
                   Tree reduce_fnc,              // reduce function
                   DataSet X,                    // left data set
                   DataSet Y,                    // right data set
                   int num_reducers,             // number of reducers
                   int n, int m,                 // dimensions of the reducer grid
                   String stop_counter )         // optional counter used in repeat operation
              throws Exception {
        String newpath = new_path(conf);
        conf.set("mrql.join.key.left",left_join_key_fnc.toString());
        conf.set("mrql.join.key.right",right_join_key_fnc.toString());
        conf.set("mrql.groupby.left",left_groupby_fnc.toString());
        conf.set("mrql.groupby.right",right_groupby_fnc.toString());
        conf.setInt("mrql.m",m);
        conf.setInt("mrql.n",n);
        conf.set("mrql.mapper",map_fnc.toString());
        conf.set("mrql.combiner",combine_fnc.toString());
        conf.set("mrql.reducer",reduce_fnc.toString());
        conf.set("mrql.counter",stop_counter);
        Job job = new Job(conf,newpath);
        distribute_compiled_arguments(job.getConfiguration());
        job.setMapOutputKeyClass(GroupByJoinKey.class);
        job.setJarByClass(GroupByJoinPlan.class);
        job.setOutputKeyClass(MRContainer.class);
        job.setOutputValueClass(MRContainer.class);
        job.setPartitionerClass(GroupByJoinPartitioner.class);
        job.setSortComparatorClass(GroupByJoinSortComparator.class);
        job.setGroupingComparatorClass(GroupByJoinGroupingComparator.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(newpath));
        for (DataSource p: X.source)
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,MapperLeft.class);
        for (DataSource p: Y.source)
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,MapperRight.class);
        job.setReducerClass(JoinReducer.class);
        if (num_reducers > 0)
            job.setNumReduceTasks(num_reducers);
        job.waitForCompletion(true);
        long c = (stop_counter.equals("-")) ? 0
                 : job.getCounters().findCounter("mrql",stop_counter).getValue();
        DataSource s = new BinaryDataSource(newpath,conf);
        s.to_be_merged = false;
        return new DataSet(s,c,MapReducePlan.outputRecords(job));
    }
}
