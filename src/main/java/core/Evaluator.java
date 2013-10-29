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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


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

    final static MR_long counter_key = new MR_long(0);
    final static MRContainer counter_container = new MRContainer(counter_key);
    final static MRContainer value_container = new MRContainer(new MR_int(0));

    /** dump MRQL data into a sequence file */
    public void dump ( String file, Tree type, MRData data ) throws Exception {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(Plan.conf);
        PrintStream ftp = new PrintStream(fs.create(path.suffix(".type")));
        ftp.print("2@"+type.toString()+"\n");
        ftp.close();
        SequenceFile.Writer writer
            = new SequenceFile.Writer(fs,Plan.conf,path,
                                      MRContainer.class,MRContainer.class);
        if (data instanceof MR_dataset)
            data = Plan.collect(((MR_dataset)data).dataset());
        if (data instanceof Bag) {
            Bag s = (Bag)data;
            long i = 0;
            for ( MRData e: s ) {
                counter_key.set(i++);
                value_container.set(e);
                writer.append(counter_container,value_container);
            }
        } else {
            counter_key.set(0);
            value_container.set(data);
            writer.append(counter_container,value_container);
        };
        writer.close();
    }

    /** dump MRQL data into a text CVS file */
    public void dump_text ( String file, Tree type, MRData data ) throws Exception {
	int ps = Config.max_bag_size_print;
	Config.max_bag_size_print = -1;
	final PrintStream out = (Config.hadoop_mode)
	                         ? Plan.print_stream(file)
	                         : new PrintStream(file);
	if (data instanceof MR_dataset)
	    data = Plan.collect(((MR_dataset)data).dataset());
	if (Translator.collection_type(type)) {
	    Tree tp = ((Node)type).children().head();
	    if (tp instanceof Node && ((Node)tp).name().equals("tuple")) {
		Trees ts = ((Node)tp).children();
		for ( MRData x: (Bag)data ) {
		    Tuple t = (Tuple)x;
		    out.print(print(t.get((short)0),ts.nth(0)));
		    for ( short i = 1; i < t.size(); i++ )
			out.print(","+print(t.get(i),ts.nth(i)));
		    out.println();
		}
	    } else for ( MRData x: (Bag)data )
		       out.println(print(x,tp));
	} else out.println(print(data,query_type));
	Config.max_bag_size_print = ps;
	out.close();
    }
}
