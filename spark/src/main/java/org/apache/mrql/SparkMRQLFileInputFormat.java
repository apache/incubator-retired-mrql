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

import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


/** A superclass for all MRQL FileInputFormats */
abstract public class SparkMRQLFileInputFormat extends FileInputFormat<MRContainer,MRContainer> implements MRQLFileInputFormat {
    public SparkMRQLFileInputFormat () {}

    /** record reader for spark */
    abstract public RecordReader<MRContainer,MRContainer>
        getRecordReader ( InputSplit split, JobConf job, Reporter reporter ) throws IOException;

    /** materialize the input file into a memory Bag */
    public Bag materialize ( final Path file ) throws IOException {
        final JobConf job = new JobConf(Plan.conf,MRQLFileInputFormat.class);
        setInputPaths(job,file);
        final InputSplit[] splits = getSplits(job,1);
        final Reporter reporter = null;
        final RecordReader<MRContainer,MRContainer> rd = getRecordReader(splits[0],job,reporter);
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
                            reader = getRecordReader(splits[i],job,reporter);
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

    /** materialize the entire dataset into a Bag
     * @param x the DataSet in HDFS to collect values from
     * @param strip is not used in MapReduce mode
     * @return the Bag that contains the collected values
     */
    public final Bag collect ( final DataSet x, boolean strip ) throws Exception {
        Bag res = new Bag();
        for ( DataSource s: x.source )
            if (s instanceof RDDDataSource)
                res = res.union(SparkEvaluator.bag(((RDDDataSource)s).rdd));
            else if (s.to_be_merged)
                res = res.union(Plan.merge(s));
            else res = res.union(s.inputFormat.newInstance().materialize(new Path(s.path)));
        return res;
    }
}
