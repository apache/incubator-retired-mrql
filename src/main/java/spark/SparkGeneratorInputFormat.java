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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;


/** the FileInputFormat for data generators: it creates HDFS files, where each file contains
 *  an (offset,size) pair that generates the range of values [offset,offset+size] */
final public class SparkGeneratorInputFormat extends SparkMRQLFileInputFormat {
    public static class GeneratorRecordReader implements RecordReader<MRContainer,MRContainer> {
        final long offset;
        final long size;
        long index;
        SequenceFile.Reader reader;

        public GeneratorRecordReader ( FileSplit split,
                                       Configuration conf ) throws IOException {
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
            reader = new SequenceFile.Reader(path.getFileSystem(conf),path,conf);
            MRContainer key = new MRContainer();
            MRContainer value = new MRContainer();
            reader.next(key,value);
            offset = ((MR_long)((Tuple)(value.data())).first()).get();
            size = ((MR_long)((Tuple)(value.data())).second()).get();
            index = -1;
        }

        public MRContainer createKey () {
            return new MRContainer(null);
        }

        public MRContainer createValue () {
            return new MRContainer(null);
        }

        public boolean next ( MRContainer key, MRContainer value ) throws IOException {
            index++;
            value.set(new MR_long(offset+index));
            key.set(new MR_long(index));
            return index < size;
        }

        public long getPos () throws IOException { return index; }

        public void close () throws IOException { reader.close(); }

        public float getProgress () throws IOException {
            return index / (float)size;
        }
    }

    public RecordReader<MRContainer,MRContainer>
              getRecordReader ( InputSplit split, JobConf job, Reporter reporter ) throws IOException {
        return (RecordReader<MRContainer,MRContainer>)
                      new GeneratorRecordReader((FileSplit)split,job);
    }

    /** Insert all results from the generators stored in path into a Bag.
     *  The Bag is lazily constructed.
     * @param path the path directory that contains the generator data (offset,size)
     * @return a Bag that contains all data
     */
    public Bag materialize ( final Path path ) throws IOException {
        Configuration conf = Plan.conf;
        FileSystem fs = path.getFileSystem(conf);
        final SequenceFile.Reader reader = new SequenceFile.Reader(path.getFileSystem(conf),path,conf);
        final MRContainer key = new MRContainer();
        final MRContainer value = new MRContainer();
        return new Bag(new BagIterator () {
                long offset = 0;
                long size = 0;
                long i = 0;
                public boolean hasNext () {
                    if (++i >= offset+size)
                        try {
                            if (!reader.next(key,value))
                                return false;
                            offset = ((MR_long)((Tuple)(value.data())).first()).get();
                            size = ((MR_long)((Tuple)(value.data())).second()).get();
                            i = offset;
                        } catch (IOException e) {
                            throw new Error("Cannot collect values from a generator");
                        };
                    return true;
                }
                public MRData next () {
                    return new MR_long(i);
                }
            });
    }
}
