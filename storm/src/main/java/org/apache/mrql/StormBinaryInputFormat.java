/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql;

import java.io.IOException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

public class StormBinaryInputFormat extends StormMRQLFileInputFormat{
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
