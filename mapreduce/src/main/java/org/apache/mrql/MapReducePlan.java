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
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;


/** A superclass for Hadoop MapReduce physical operators (files *Operator.java) */
public class MapReducePlan extends Plan {

    MapReducePlan () {}

    /** find the number of records in the hadoop MapReduce job output */
    public final static long outputRecords ( Job job ) throws Exception {
        CounterGroup cg = job.getCounters().getGroup("org.apache.hadoop.mapred.Task$Counter");
        long rc = cg.findCounter("REDUCE_OUTPUT_RECORDS").getValue();
        if (rc == 0)
            return cg.findCounter("MAP_OUTPUT_RECORDS").getValue();
        return rc;
    }

    /** The Aggregate physical operator
     * @param acc_fnc  the accumulator function from (T,T) to T
     * @param zero  the zero element of type T
     * @param S the dataset that contains the bag of values {T}
     * @return the aggregation result of type T
     */
    public final static MRData aggregate ( final Tree acc_fnc,
                                           final Tree zero,
                                           final DataSet S ) throws Exception {
        MRData res = Interpreter.evalE(zero);
        Function accumulator = functional_argument(Plan.conf,acc_fnc);
        Tuple pair = new Tuple(2);
        for ( DataSource s: S.source )
            if (s.inputFormat != MapReduceBinaryInputFormat.class) {
                pair.set(0,res);
                pair.set(1,aggregate(acc_fnc,zero,
                                     MapOperation.cMap(Interpreter.identity_mapper,acc_fnc,zero,
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

    /** The repeat physical operator. It repeats the loop until all values satisfy
     *    the termination condition (when dataset.counter == 0)
     * @param loop the function from DataSet to DataSet to be repeated
     * @param init the initial input DataSet for the loop
     * @param max_num max number of repetitions
     * @return the resulting DataSet from the loop
     */
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

    /** The closure physical operator. It repeats the loop until the size
     *    of the new DataSet is equal to that of the previous DataSet
     * @param loop the function from DataSet to DataSet to be repeated
     * @param init the initial input DataSet for the loop
     * @param max_num max number of repetitions
     * @return the resulting DataSet from the loop
     */
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
