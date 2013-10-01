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

import org.apache.mrql.gen.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;


/** A data source for a text HDFS file along with the parser to parse it */
public class ParsedDataSource extends DataSource {
    public Class<? extends Parser> parser;
    public Trees args;

    ParsedDataSource ( int source_num,
                       String path,
                       Class<? extends Parser> parser,
                       Trees args,
                       Configuration conf ) {
        super(source_num,path,Evaluator.evaluator.parsedInputFormat(),conf);
        this.parser = parser;
        this.args = args;
    }

    ParsedDataSource ( String path,
                       Class<? extends Parser> parser,
                       Trees args,
                       Configuration conf ) {
        super(-1,path,Evaluator.evaluator.parsedInputFormat(),conf);
        this.parser = parser;
        this.args = args;
    }

    public String toString () {
        try {
            String pn = "";
            for ( String k: DataSource.parserDirectory.keySet() )
                if (DataSource.parserDirectory.get(k).equals(parser))
                    pn = k;
            return "Text"+separator+source_num+separator+pn+separator+path
                   +separator+(new Node("args",args)).toString();
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
