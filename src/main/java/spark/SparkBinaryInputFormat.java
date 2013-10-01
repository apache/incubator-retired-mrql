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
import org.apache.hadoop.mapred.*;


/** Input format for hadoop sequence files */
final public class SparkBinaryInputFormat extends SparkMRQLFileInputFormat {
    public static class BinaryInputRecordReader extends SequenceFileRecordReader<MRContainer,MRContainer> {
        final MRContainer result = new MRContainer();

        public BinaryInputRecordReader ( FileSplit split,
                                         JobConf job ) throws IOException {
            super(job,split);
        }

        @Override
        public synchronized boolean next ( MRContainer key, MRContainer value ) throws IOException {
                boolean b = super.next(key,result);
                value.set(result.data());
                return b;
        }
    }

    @Override
    public RecordReader<MRContainer,MRContainer>
              getRecordReader ( InputSplit split,
                                JobConf job,
                                Reporter reporter ) throws IOException {
        String path = ((FileSplit)split).getPath().toString();
        BinaryDataSource ds = (BinaryDataSource)DataSource.get(path,job);
        return new BinaryInputRecordReader((FileSplit)split,job);
    }
}
