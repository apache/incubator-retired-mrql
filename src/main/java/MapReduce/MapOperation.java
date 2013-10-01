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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/** The MapReduce operation that use a map stage only */
final public class MapOperation extends MapReducePlan {

    /** The mapper of Map */
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

    /** The cMap physical operator
     * @param map_fnc       mapper function
     * @param acc_fnc       optional accumulator function
     * @param zero          optional the zero value for the accumulator
     * @param source        input data source
     * @param stop_counter  optional counter used in repeat operation
     * @return a new data source that contains the result
     */
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
            MultipleInputs.addInputPath(job,new Path(p.path),(Class<? extends MapReduceMRQLFileInputFormat>)p.inputFormat,cMapMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(newpath));
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
        long c = (stop_counter.equals("-")) ? 0
                 : job.getCounters().findCounter("mrql",stop_counter).getValue();
        return new DataSet(new BinaryDataSource(newpath,conf),c,outputRecords(job));
    }
}
