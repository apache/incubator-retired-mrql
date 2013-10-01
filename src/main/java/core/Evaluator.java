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

import java_cup.runtime.*;
import org.apache.mrql.gen.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;


/** Evaluates physical plans using one of the evaluation engines */
abstract public class Evaluator extends Interpreter {

    /** the current MRQL evaluator */
    public static Evaluator evaluator;

    /** initialize the evaluator */
    abstract public void init ( Configuration conf );

    /** shutdown the evaluator */
    abstract public void shutdown ( Configuration conf );

    /** initialize the query evaluation */
    abstract public void initialize_query ();

    /** create a new evaluation configuration */
    abstract public Configuration new_configuration ();

    /** synchronize peers in BSP mode */
    public MR_bool synchronize ( MR_string peerName, MR_bool mr_exit ) {
        throw new Error("You can only synchronize BSP tasks");
    }

    /** distribute a bag among peers in BSP mode */
    public Bag distribute ( MR_string peerName, Bag s ) {
        throw new Error("You can only distribute bags among BSP tasks");
    }

    /** run a BSP task */
    public MRData bsp ( Tree plan, Environment env ) throws Exception {
        throw new Error("You can only run a BSP task in BSP mode");
    }

    /** collect all data from a persistent dataset into a Bag */
    abstract public Bag collect ( final DataSet x ) throws Exception;

    /** return the FileInputFormat for parsed files (CSV, XML, JSON, etc) */
    abstract public Class<? extends MRQLFileInputFormat> parsedInputFormat ();

    /** return the FileInputFormat for binary files */
    abstract public Class<? extends MRQLFileInputFormat> binaryInputFormat ();

    /** return the FileInputFormat for data generator files */
    abstract public Class<? extends MRQLFileInputFormat> generatorInputFormat ();

    /** The Aggregate physical operator
     * @param acc_fnc  the accumulator function from (T,T) to T
     * @param zero  the zero element of type T
     * @param plan the plan that constructs the dataset that contains the bag of values {T}
     * @param env contains bindings fro variables to values (MRData)
     * @return the aggregation result of type T
     */
    abstract public MRData aggregate ( Tree acc_fnc,
                                       Tree zero,
                                       Tree plan,
                                       Environment env ) throws Exception;

    /** Evaluate a loop a fixed number of times */
    abstract public Tuple loop ( Tree e, Environment env ) throws Exception;

    /** Evaluate a MRQL physical plan and print tracing info
     * @param e the physical plan
     * @param env contains bindings fro variables to values (MRData)
     * @return a DataSet (stored in HDFS)
     */
    abstract public DataSet eval ( final Tree e,
                                   final Environment env,
                                   final String counter );
}
