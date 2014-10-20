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
import java.util.List;
import java.util.ArrayList;
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;


/** Input format for Hadoop Sequence files */
final public class FlinkSequenceInputFormat extends FlinkMRQLFileInputFormat {
    public FlinkSequenceInputFormat () {}

    public static final class FDataInputFormat extends BinaryInputFormat<FData> {
        private SequenceFile.Reader in;
        private long start;
        private long end;
        private boolean more = true;
        MRContainer key = new MRContainer(new MR_int(0));
        MRContainer value = new MRContainer(new MR_int(0));

        @Override
        public void open ( FileInputSplit split ) throws IOException {
            Path path = new Path(split.getPath().toString());
	    if (Plan.conf == null)
		Plan.conf = new Configuration();
            FileSystem fs = path.getFileSystem(Plan.conf);
            in = new SequenceFile.Reader(fs,path,Plan.conf);
            end = split.getStart()+split.getLength();
            if (split.getStart() > in.getPosition())
                in.sync(split.getStart());       // sync to start
            start = in.getPosition();
            more = start < end;
        }

        @Override
        public FData nextRecord ( FData record ) throws IOException {
            if (!more)
                return null;
            long pos = in.getPosition();
            if (!in.next(key) || (pos >= end && in.syncSeen())) {
                more = false;
                return null;
            };
            in.getCurrentValue(value);
            return new FData(value.data());
        }

        @Override
        public boolean reachedEnd () {
            return !more;
        }

        @Override
        public void close () throws IOException {
            super.close();
            in.close();
        }

       @Override
        protected List<FileStatus> getFiles () throws IOException {
            List<FileStatus> files = super.getFiles();
            List<FileStatus> sfiles = new ArrayList<FileStatus>();
            for ( FileStatus file: files ) {
                String fname = file.getPath().getName();
                if (!fname.startsWith("_") && !fname.endsWith(".crc"))
                    sfiles.add(file);
            };
            return sfiles;
        }

        @Override
        protected FData deserialize ( FData reuse, DataInputView in ) throws IOException {
            reuse.read(in);
            return reuse;
        }
    }

    /** the Flink input format for this input */
    public FileInputFormat<FData> inputFormat ( String path  ) {
        FDataInputFormat sf = new FDataInputFormat();
        sf.setFilePath(path.toString());
        return sf;
    }
}
