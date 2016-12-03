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
import java.io.Serializable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public abstract class StormMRQLFileInputFormat extends FileInputFormat<MRContainer,MRContainer> implements MRQLFileInputFormat,Serializable {

    abstract public RecordReader<MRContainer,MRContainer>
    getRecordReader ( InputSplit split, JobConf job, Reporter reporter ) throws IOException;

    @Override
    public Bag materialize(final Path file) throws IOException {
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

    @Override
    public Bag collect(DataSet x, boolean strip) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
