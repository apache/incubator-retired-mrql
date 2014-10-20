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
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.api.common.io.FileInputFormat;


/** Input format for binary files */
final public class FlinkBinaryInputFormat extends FlinkMRQLFileInputFormat {
    public FlinkBinaryInputFormat () {}

    public static final class FDataInputFormat extends BinaryInputFormat<FData> {
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
