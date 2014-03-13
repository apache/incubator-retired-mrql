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

import org.apache.hadoop.conf.Configuration;


/** A DataSource used for storing intermediate results and data dumps */
final public class BinaryDataSource extends DataSource {
    BinaryDataSource ( int source_num, String path, Configuration conf ) {
        super(source_num,path,Evaluator.evaluator.binaryInputFormat(),conf);
    }

    BinaryDataSource ( String path, Configuration conf ) {
        super(-1,path,Evaluator.evaluator.binaryInputFormat(),conf);
    }

    public String toString () {
        return "Binary"+separator+source_num+separator+path;
    }
}
