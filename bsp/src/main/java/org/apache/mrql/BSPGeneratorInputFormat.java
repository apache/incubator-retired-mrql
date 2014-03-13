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

import org.apache.mrql.gen.Trees;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.*;


/** the FileInputFormat for data generators: it creates HDFS files, where each file contains
 *  an (offset,size) pair that generates the range of values [offset,offset+size] */
final public class BSPGeneratorInputFormat extends BSPMRQLFileInputFormat {
    public static class GeneratorRecordReader implements RecordReader<MRContainer,MRContainer> {
        final long offset;
        final long size;
        final int source_number;
        final MRData source_num_data;
        long index;
        SequenceFile.Reader reader;

        public GeneratorRecordReader ( FileSplit split,
                                       int source_number,
                                       BSPJob job ) throws IOException {
            Configuration conf = BSPPlan.getConfiguration(job);
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
            reader = new SequenceFile.Reader(path.getFileSystem(conf),path,conf);
            MRContainer key = new MRContainer();
            MRContainer value = new MRContainer();
            reader.next(key,value);
            offset = ((MR_long)((Tuple)(value.data())).first()).get();
            size = ((MR_long)((Tuple)(value.data())).second()).get();
            this.source_number = source_number;
            source_num_data = new MR_int(source_number);
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
            value.set(new Tuple(source_num_data,new MR_long(offset+index)));
            key.set(new MR_long(index));
            return index < size;
        }

        public long getPos () throws IOException { return index; }

        public void close () throws IOException { reader.close(); }

        public float getProgress () throws IOException {
            return index / (float)size;
        }

        public void initialize ( InputSplit split, TaskAttemptContext context ) throws IOException { }
    }

    public RecordReader<MRContainer,MRContainer>
                getRecordReader ( InputSplit split, BSPJob job ) throws IOException {
        Configuration conf = BSPPlan.getConfiguration(job);
        String path = ((FileSplit)split).getPath().toString();
        GeneratorDataSource ds = (GeneratorDataSource)DataSource.get(path,conf);
        return new GeneratorRecordReader((FileSplit)split,ds.source_num,job);
    }
}
