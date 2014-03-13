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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


/** Input format for Apache Hadoop sequence files */
final public class MapReduceBinaryInputFormat extends MapReduceMRQLFileInputFormat {
    final static SequenceFileInputFormat<MRContainer,MRContainer> inputFormat
                               = new SequenceFileInputFormat<MRContainer,MRContainer>();

    public RecordReader<MRContainer,MRContainer>
              createRecordReader ( InputSplit split,
                                   TaskAttemptContext context ) throws IOException, InterruptedException {
        return inputFormat.createRecordReader(split,context);
    }

    /** collect the data from multiple sequence files at the path directory into a Bag
     * @param path the path directory
     * @return a Bag that contains all data
     */
    public Bag materialize ( final Path path ) throws IOException {
        final FileSystem fs = path.getFileSystem(Plan.conf);
        final FileStatus[] ds
                   = fs.listStatus(path,
                                   new PathFilter () {
                                       public boolean accept ( Path path ) {
                                           return !path.getName().startsWith("_");
                                       }
                                   });
        if (ds.length > 0)
            return new Bag(new BagIterator () {
                    SequenceFile.Reader reader = new SequenceFile.Reader(fs,ds[0].getPath(),Plan.conf);
                    MRContainer key = new MRContainer(new MR_int(0));
                    MRContainer value = new MRContainer(new MR_int(0));
                    int i = 1;
                    public boolean hasNext () {
                        try {
                            if (reader.next(key,value))
                                return true;
                            do {
                                if (i >= ds.length)
                                    return false;
                                reader.close();
                                reader = new SequenceFile.Reader(fs,ds[i++].getPath(),Plan.conf);
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
        return new Bag();
    }
}
