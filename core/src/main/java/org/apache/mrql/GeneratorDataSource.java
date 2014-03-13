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

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;


/** A DataSource used for processing the range min..max */
final public class GeneratorDataSource extends DataSource {
    GeneratorDataSource ( int source_num, String path, Configuration conf ) {
        super(source_num,path,Evaluator.evaluator.generatorInputFormat(),conf);
    }

    GeneratorDataSource ( String path, Configuration conf ) {
        super(-1,path,Evaluator.evaluator.generatorInputFormat(),conf);
    }

    public static long size ( Path path, Configuration conf ) throws IOException {
        // each file generates range_split_size long integers
        FileStatus s = path.getFileSystem(conf).getFileStatus(path);
        if (!s.isDir())
            return Config.range_split_size*8;
        long size = 0;
        for ( FileStatus fs: path.getFileSystem(conf).listStatus(path) )
            size += Config.range_split_size*8;
        return size;
    }

    public String toString () {
        return "Generator"+separator+source_num+separator+path;
    }
}
