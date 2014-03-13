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
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;


/** Test all the MRQL test queries */
final public class Test {
    public static PrintStream print_stream;
    public static Configuration conf;
    static MRQLParser parser = new MRQLParser();
    static String result_directory;
    static PrintStream test_out;
    static PrintStream error_stream;

    private static int compare ( String file1, String file2 ) throws Exception {
        FileInputStream s1 = new FileInputStream(file1);
        FileInputStream s2 = new FileInputStream(file2);
        int b1, b2;
        int i = 1;
        while ((b1 = s1.read()) == (b2 = s2.read()) && b1 != -1 && b2 != -1)
            i++;
        return (b1 == -1 && b2 == -1) ? 0 : i;
    }

    private static void query ( File query ) throws Exception {
        String path = query.getPath();
        if (!path.endsWith(".mrql"))
            return;
        Translator.global_reset();
        String qname = query.getName();
        qname = qname.substring(0,qname.length()-5);
        test_out.print("   Testing "+qname+" ... ");
        String result_file = result_directory+"/"+qname+".txt";
        boolean exists = new File(result_file).exists();
        if (exists)
            System.setOut(new PrintStream(result_directory+"/result.txt"));
        else System.setOut(new PrintStream(result_file));
        try {
            parser = new MRQLParser(new MRQLLex(new FileInputStream(query)));
            Main.parser = parser;
            MRQLLex.reset();
            parser.parse();
            int i;
            if (exists && (i = compare(result_file,result_directory+"/result.txt")) > 0)
                test_out.println("MISMATCH AT "+(i-1));
            else if (exists)
                test_out.println("OK matched");
            else test_out.println("OK created");
        } catch (Error ex) {
            error_stream.println(qname+": "+ex);
            ex.printStackTrace(error_stream);
            test_out.println("FAILED");
            if (!exists)
                new File(result_file).delete();
        } catch (Exception ex) {
            error_stream.println(qname+": "+ex);
            ex.printStackTrace(error_stream);
            test_out.println("FAILED");
            if (!exists)
                new File(result_file).delete();
        } finally {
            if (Config.hadoop_mode)
                Plan.clean();
            if (Config.compile_functional_arguments)
                Compiler.clean();
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
        Config.quiet_execution = true;
        if (hadoop) {
            conf = Evaluator.evaluator.new_configuration();
            GenericOptionsParser gop = new GenericOptionsParser(conf,args);
            conf = gop.getConfiguration();
            args = gop.getRemainingArgs();
        };
        Config.parse_args(args,conf);
        Config.hadoop_mode = Config.local_mode || Config.distributed_mode;
        Evaluator.evaluator.init(conf);
        new TopLevel();
        Config.testing = true;
        if (Config.hadoop_mode && Config.bsp_mode)
            Config.write(Plan.conf);
        if (Main.query_file.equals("") || Config.extra_args.size() != 2)
            throw new Error("Must provide a query directory, a result directory, and an error log file");
        File query_dir = new File(Main.query_file);
        result_directory = Config.extra_args.get(0);
        (new File(result_directory)).mkdirs();
        error_stream = new PrintStream(Config.extra_args.get(1));
        System.setErr(error_stream);
        test_out = System.out;
        for ( File f: query_dir.listFiles() )
            query(f);
    }
}
