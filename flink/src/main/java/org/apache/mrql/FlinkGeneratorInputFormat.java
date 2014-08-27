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

import org.apache.flink.api.common.io.FileInputFormat;


/** the FileInputFormat for data generators: it creates HDFS files, where each file contains
 *  an (offset,size) pair that generates the range of values [offset,offset+size] */
final public class FlinkGeneratorInputFormat extends FlinkMRQLFileInputFormat {
    /** the Flink input format for this input */
    public FileInputFormat<FData> inputFormat ( String path ) { return null; }
}
