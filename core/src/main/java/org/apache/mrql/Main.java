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
import java.util.Enumeration;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import jline.*;


final public class Main {
    public final static String version = "0.9.0";

    public static PrintStream print_stream;
    public static Configuration conf;
    static MRQLParser parser = new MRQLParser();
    public static String query_file = "";

    public static void include_file ( String file ) {
        try {
            MRQLParser old_parser = parser;
            boolean old_interactive = Config.interactive;
            try {
                parser = new MRQLParser(new MRQLLex(new FileInputStream(file)));
            } catch (Exception e) {
                Path path = new Path(file);
                FileSystem fs = path.getFileSystem(conf);
                parser = new MRQLParser(new MRQLLex(fs.open(path)));
            };
            Config.interactive = false;
            parser.parse();
            Config.interactive = old_interactive;
            parser = old_parser;
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            throw new Error(ex);
        }
    }

    public static void main ( String[] args ) throws Exception {
        boolean hadoop = false;
        for ( String arg: args ) {
            hadoop |= arg.equals("-local") || arg.equals("-dist");
            Config.bsp_mode |= arg.equals("-bsp");
            Config.spark_mode |= arg.equals("-spark");
        };
        Config.map_reduce_mode = !Config.bsp_mode && !Config.spark_mode;
        if (Config.map_reduce_mode)
            Evaluator.evaluator = (Evaluator)Class.forName("org.apache.mrql.MapReduceEvaluator").newInstance();
        if (Config.bsp_mode)
            Evaluator.evaluator = (Evaluator)Class.forName("org.apache.mrql.BSPEvaluator").newInstance();
        if (Config.spark_mode)
            Evaluator.evaluator = (Evaluator)Class.forName("org.apache.mrql.SparkEvaluator").newInstance();
        if (hadoop) {
            conf = Evaluator.evaluator.new_configuration();
            GenericOptionsParser gop = new GenericOptionsParser(conf,args);
            conf = gop.getConfiguration();
            args = gop.getRemainingArgs();
        };
        Config.parse_args(args,conf);
        Config.hadoop_mode = Config.local_mode || Config.distributed_mode;
        if (!Config.info) {
            for ( Enumeration en = LogManager.getCurrentLoggers(); en.hasMoreElements(); )
                ((Logger)en.nextElement()).setLevel(Level.WARN);
            LogManager.getRootLogger().setLevel(Level.WARN);
        };
        Evaluator.evaluator.init(conf);
        new TopLevel();
        System.out.print("Apache MRQL version "+version+" (");
        if (Config.compile_functional_arguments)
            System.out.print("compiled ");
        else System.out.print("interpreted ");
        if (hadoop) {
            if (Config.local_mode)
                System.out.print("local ");
            else if (Config.distributed_mode)
                System.out.print("distributed ");
            if (Config.spark_mode)
                System.out.println("Spark mode using "+Config.nodes+" tasks)");
            else if (Config.bsp_mode)
                System.out.println("Hama BSP mode over "+Config.nodes+" BSP tasks)");
            else if (Config.nodes > 0)
                System.out.println("Hadoop MapReduce mode with "+Config.nodes+" reducers)");
            else if (!Config.local_mode)
                System.out.println("Hadoop MapReduce mode with 1 reducer, use -nodes to change it)");
            else System.out.println("Hadoop MapReduce mode)");
        } else if (Config.bsp_mode)
            System.out.println("in-memory BSP mode)");
        else System.out.println("in-memory MapReduce mode)");
        if (Config.interactive) {
            System.out.println("Type quit to exit");
            ConsoleReader reader = new ConsoleReader();
            reader.setBellEnabled(false);
            History history = new History(new File(System.getProperty("user.home")+"/.mrqlhistory"));
            reader.setHistory(history);
            reader.setUseHistory(false);
            try {
            loop: while (true) {
                String line = "";
                String s = "";
                try {
                    if (hadoop && Config.bsp_mode)
                        Config.write(Plan.conf);
                    do {
                        s = reader.readLine("> ");
                        if (s != null && (s.equals("quit") || s.equals("exit")))
                            break loop;
                        if (s != null)
                            line += " "+s;
                    } while (s == null || s.indexOf(";") <= 0);
                    line = line.substring(1);
                    history.addToHistory(line);
                    parser = new MRQLParser(new MRQLLex(new StringReader(line)));
                    MRQLLex.reset();
                    parser.parse();
                } catch (EOFException x) {
                    break;
                } catch (Exception x) {
                    if (x.getMessage() != null)
                        System.out.println(x);
                } catch (Error x) {
                    System.out.println(x);
                }
                }
            } finally {
                if (hadoop) {
                    Plan.clean();
                    Evaluator.evaluator.shutdown(Plan.conf);
                };
                if (Config.compile_functional_arguments)
                    Compiler.clean();
            }
        } else try {
                if (hadoop && Config.bsp_mode)
                    Config.write(Plan.conf);
                try {
                    parser = new MRQLParser(new MRQLLex(new FileInputStream(query_file)));
                } catch (Exception e) {
                    // when the query file is in HDFS
                    Path path = new Path(query_file);
                    FileSystem fs = path.getFileSystem(conf);
                    parser = new MRQLParser(new MRQLLex(fs.open(path)));
                };
                parser.parse();
            } finally {
                if (hadoop) {
                    Plan.clean();
                    Evaluator.evaluator.shutdown(Plan.conf);
                };
                if (Config.compile_functional_arguments)
                    Compiler.clean();
            }
    }
}
