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


/** A data source for a text HDFS file along with the parser to parse it */
public class FlinkParsedDataSource extends ParsedDataSource {
    FlinkParsedDataSource ( String path,
                            Class<? extends Parser> parser,
                            Trees args ) {
        this.source_num = -1;
        this.path = path;
        this.inputFormat = FlinkParsedInputFormat.class;
        to_be_merged = false;
        this.parser = parser;
        this.args = args;
        try {
            dataSourceDirectory.put(path,this);
        } catch (Exception ex) {
            throw new Error("Cannot open file: "+path);
        }
    }
}
