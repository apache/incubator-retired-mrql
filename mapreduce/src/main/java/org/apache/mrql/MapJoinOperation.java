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
import java.net.URI;
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
import org.apache.hadoop.filecache.DistributedCache;


/** The fragment-replicate join (map-side join) physical operator */
final public class MapJoinOperation extends MapReducePlan {

    /** the mapper of the MapJoin */
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

    /** The fragment-replicate join (map-side join) physical operator
     * @param probe_map_fnc    left mapper function
     * @param built_map_fnc    right mapper function
     * @param reduce_fnc       reducer function
     * @param acc_fnc          optional accumulator function
     * @param zero             optional the zero value for the accumulator
     * @param probe_dataset    the map source
     * @param built_dataset    stored in distributed cache
     * @param stop_counter     optional counter used in repeat operation
     * @return a new data source that contains the result
     */
    public final static DataSet mapJoin ( Tree probe_map_fnc,    // left mapper function
                                          Tree built_map_fnc,    // right mapper function
                                          Tree reduce_fnc,       // reducer function
                                          Tree acc_fnc,          // optional accumulator function
                                          Tree zero,             // optional the zero value for the accumulator
                                          DataSet probe_dataset, // the map source
                                          DataSet built_dataset, // stored in distributed cache
                                          String stop_counter )  // optional counter used in repeat operation
                                throws Exception {
        DataSet ds = MapOperation.cMap(built_map_fnc,null,null,built_dataset,"-");
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
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,mapJoinMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(newpath));
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
        long c = (stop_counter.equals("-")) ? 0
                 : job.getCounters().findCounter("mrql",stop_counter).getValue();
        return new DataSet(new BinaryDataSource(newpath,conf),c,outputRecords(job));
    }
}
