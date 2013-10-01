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
import java.util.List;
import java.util.Vector;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;


/** The CrossProduct physical operation (similar to block-nested loop) */
final public class CrossProductOperation extends MapReducePlan {

    /** The mapper for the CrossProduct operation */
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

    /** The CrossProduct physical operator (similar to block-nested loop)
     * @param mx              left mapper
     * @param my              right mapper
     * @param reduce_fnc      reducer
     * @param acc_fnc         optional accumulator function
     * @param zero            optional the zero value for the accumulator
     * @param X               the left source
     * @param Y               the right source (stored in distributed cache)
     * @param stop_counter    optional counter used in repeat operation
     * @return a new data source that contains the result
     */
    public final static DataSet crossProduct ( Tree mx,              // left mapper
                                               Tree my,              // right mapper
                                               Tree reduce_fnc,      // reducer
                                               Tree acc_fnc,         // optional accumulator function
                                               Tree zero,            // optional the zero value for the accumulator
                                               DataSet X,            // the left source
                                               DataSet Y,            // the right source (stored in distributed cache)
                                               String stop_counter ) // optional counter used in repeat operation
                                 throws Exception {
        DataSet ds = MapOperation.cMap(my,null,null,Y,"-");
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
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,crossProductMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(newpath));
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
        long c = (stop_counter.equals("-")) ? 0
                 : job.getCounters().findCounter("mrql",stop_counter).getValue();
        return new DataSet(new BinaryDataSource(newpath,conf),c,outputRecords(job));
    }
}
