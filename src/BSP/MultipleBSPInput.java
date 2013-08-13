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


/** A FileInputFormat for multiple files, where each file may be associated with
 *   a different FileInputFormat */
final public class MultipleBSPInput extends MRQLFileInputFormat {
    public RecordReader<MRContainer,MRContainer>
	      getRecordReader ( InputSplit split, BSPJob job ) throws IOException {
	String path = ((FileSplit)split).getPath().toString();
	Configuration conf = BSPPlan.getConfiguration(job);
	DataSource ds = DataSource.get(path,conf);
	Plan.conf = conf;
	if (ds instanceof ParsedDataSource)
	    return new ParsedInputFormat.ParsedRecordReader((FileSplit)split,
							    job,
							    ((ParsedDataSource)ds).parser,
							    ds.source_num,
							    (Trees)((ParsedDataSource)ds).args);
	else if (ds instanceof BinaryDataSource)
	    return new BinaryInputFormat.BinaryInputRecordReader((FileSplit)split,job,ds.source_num);
	else if (ds instanceof GeneratorDataSource)
	    return new GeneratorInputFormat.GeneratorRecordReader((FileSplit)split,ds.source_num,job);
	else throw new Error("Unknown data source: "+ds+" for path "+path);
    }
}
