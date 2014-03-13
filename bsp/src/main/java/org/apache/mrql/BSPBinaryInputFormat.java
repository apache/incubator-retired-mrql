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
import org.apache.hama.HamaConfiguration;


/** Input format for hadoop sequence files */
final public class BSPBinaryInputFormat extends BSPMRQLFileInputFormat {
    public static class BinaryInputRecordReader extends SequenceFileRecordReader<MRContainer,MRContainer> {
        final MRContainer result = new MRContainer();
        final MRData source_num_data;
        final int source_number;

        public BinaryInputRecordReader ( FileSplit split,
                                         BSPJob job,
                                         int source_number ) throws IOException {
            super(BSPPlan.getConfiguration(job),split);
            this.source_number = source_number;
            source_num_data = new MR_int(source_number);
        }

        @Override
        public synchronized boolean next ( MRContainer key, MRContainer value ) throws IOException {
                boolean b = super.next(key,result);
                value.set(new Tuple(source_num_data,result.data()));
                return b;
        }
    }

    public RecordReader<MRContainer,MRContainer>
              getRecordReader ( InputSplit split,
                                BSPJob job ) throws IOException {
        Configuration conf = BSPPlan.getConfiguration(job);
        String path = ((FileSplit)split).getPath().toString();
        BinaryDataSource ds = (BinaryDataSource)DataSource.get(path,conf);
        return new BinaryInputRecordReader((FileSplit)split,job,ds.source_num);
    }
}
