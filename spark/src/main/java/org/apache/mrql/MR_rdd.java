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
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaRDD;


/** a wrapper of a JavaRDD<MRData> (stored in HDFS) as an MRData */
final public class MR_rdd extends MRData {
    public JavaRDD<MRData> rdd;

    public MR_rdd ( JavaRDD<MRData> d ) { rdd = d; }

    public void materializeAll () {};

    public JavaRDD<MRData> rdd () { return rdd; }

    final public void write ( DataOutput out ) throws IOException {
        throw new Error("RDDs are not serializable");
    }

    public void readFields ( DataInput in ) throws IOException {
        throw new Error("RDDs are not serializable");
    }

    public int compareTo ( MRData x ) {
        throw new Error("RDDs cannot be compared");
    }

    public boolean equals ( Object x ) {
        throw new Error("RDDs cannot be compared");
    }
}
