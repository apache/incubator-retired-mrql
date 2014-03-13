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

import org.apache.mrql.gen.VariableLeaf;
import java.util.ArrayList;
import java.io.FileInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/** MRQL configuration parameters */
final public class Config {
    public static boolean loaded = false;

    // true for using Hadoop HDFS file-system
    public static boolean hadoop_mode = false;
    // true for local execution (one node)
    public static boolean local_mode = false;
    // true for local execution (one node)
    public static boolean distributed_mode = false;
    // true for Hadoop map-reduce mode
    public static boolean map_reduce_mode = false;
    // true, for BSP mode using Hama
    public static boolean bsp_mode = false;
    // true, for Spark mode
    public static boolean spark_mode = false;
    // if true, it process the input interactively
    public static boolean interactive = true;
    // compile the MR functional arguments to Java bytecode at run-time
    // (each task-tracker repeats the compilation at the MR setup time)
    public static boolean compile_functional_arguments = true;
    // if true, generates info about all compilation and optimization steps
    public static boolean trace = false;
    // number of worker nodes
    public static int nodes = 2;
    // true, to disable mapJoin
    public static boolean noMapJoin = false;
    // max distributed cache size for MapJoin (fragment-replicate join) in MBs
    public static int mapjoin_size = 50;
    // max entries for in-mapper combiner before they are flushed out
    public static int map_cache_size = 100000;
    // max number of bag elements to print
    public static int max_bag_size_print = 20;
    // max size of materialized vector before is spilled to a file:
    public static int max_materialized_bag = 500000;
    // max number of incoming messages before a sub-sync()
    public static int bsp_msg_size = Integer.MAX_VALUE;
    // number of elements per mapper to process the range min...max
    public static long range_split_size = 100000;
    // max number of streams to merge simultaneously
    public static int max_merged_streams = 100;
    // the directory for temporary files and spilled bags
    public static String tmpDirectory = "/tmp/mrql_"+System.getProperty("user.name");
    // true, if we want to derive a combine function for MapReduce
    public static boolean use_combiner = true;
    // true, if we can use the rule that fuses a groupBy with a join over the same key
    public static boolean groupJoinOpt = true;
    // true, if we can use the rule that converts a self-join into a simple mapreduce
    public static boolean selfJoinOpt = true;
    // true for run-time trace of plans
    public static boolean trace_execution = false;
    // true for extensive run-time trace of expressions & plans
    public static boolean trace_exp_execution = false;
    // true if you don't want to print statistics
    public static boolean quiet_execution = false;
    // true if this is during testing
    public static boolean testing = false;
    // true to display INFO log messages
    public static boolean info = false;

    /** store the configuration parameters */
    public static void write ( Configuration conf ) {
        conf.setBoolean("mrql.hadoop.mode",hadoop_mode);
        conf.setBoolean("mrql.local.mode",local_mode);
        conf.setBoolean("mrql.distributed.mode",distributed_mode);
        conf.setBoolean("mrql.map.reduce.mode",map_reduce_mode);
        conf.setBoolean("mrql.bsp.mode",bsp_mode);
        conf.setBoolean("mrql.spark.mode",spark_mode);
        conf.setBoolean("mrql.interactive",interactive);
        conf.setBoolean("mrql.compile.functional.arguments",compile_functional_arguments);
        conf.setBoolean("mrql.trace",trace);
        conf.setInt("mrql.nodes",nodes);
        conf.setInt("mrql.mapjoin.size",mapjoin_size);
        conf.setInt("mrql.in.mapper.size",map_cache_size);
        conf.setInt("mrql.max.bag.size.print",max_bag_size_print);
        conf.setInt("mrql.max.materialized.bag",max_materialized_bag);
        conf.setInt("mrql.bsp.msg.size",bsp_msg_size);
        conf.setLong("mrql.range.split.size",range_split_size);
        conf.setInt("mrql.max.merged.streams",max_merged_streams);
        conf.set("mrql.tmp.directory",tmpDirectory);
        conf.setBoolean("mrql.use.combiner",use_combiner);
        conf.setBoolean("mrql.group.join.opt",groupJoinOpt);
        conf.setBoolean("mrql.self.join.opt",selfJoinOpt);
        conf.setBoolean("mrql.trace.execution",trace_execution);
        conf.setBoolean("mrql.trace.exp.execution",trace_exp_execution);
        conf.setBoolean("mrql.quiet.execution",quiet_execution);
        conf.setBoolean("mrql.testing",testing);
        conf.setBoolean("mrql.info",info);
    }

    /** load the configuration parameters */
    public static void read ( Configuration conf ) {
        if (loaded)
            return;
        loaded = true;
        hadoop_mode = conf.getBoolean("mrql.hadoop.mode",hadoop_mode);
        local_mode = conf.getBoolean("mrql.local.mode",local_mode);
        distributed_mode = conf.getBoolean("mrql.distributed.mode",distributed_mode);
        map_reduce_mode = conf.getBoolean("mrql.map.reduce.mode",map_reduce_mode);
        bsp_mode = conf.getBoolean("mrql.bsp.mode",bsp_mode);
        spark_mode = conf.getBoolean("mrql.spark.mode",spark_mode);
        interactive = conf.getBoolean("mrql.interactive",interactive);
        compile_functional_arguments = conf.getBoolean("mrql.compile.functional.arguments",compile_functional_arguments);
        trace = conf.getBoolean("mrql.trace",trace);
        nodes = conf.getInt("mrql.nodes",nodes);
        mapjoin_size = conf.getInt("mrql.mapjoin.size",mapjoin_size);
        map_cache_size = conf.getInt("mrql.in.mapper.size",map_cache_size);
        max_bag_size_print = conf.getInt("mrql.max.bag.size.print",max_bag_size_print);
        max_materialized_bag = conf.getInt("mrql.max.materialized.bag",max_materialized_bag);
        bsp_msg_size = conf.getInt("mrql.bsp.msg.size",bsp_msg_size);
        range_split_size = conf.getLong("mrql.range.split.size",range_split_size);
        max_merged_streams = conf.getInt("mrql.max.merged.streams",max_merged_streams);
        tmpDirectory = conf.get("mrql.tmp.directory");
        use_combiner = conf.getBoolean("mrql.use.combiner",use_combiner);
        groupJoinOpt = conf.getBoolean("mrql.group.join.opt",groupJoinOpt);
        selfJoinOpt = conf.getBoolean("mrql.self.join.opt",selfJoinOpt);
        trace_execution = conf.getBoolean("mrql.trace.execution",trace_execution);
        trace_exp_execution = conf.getBoolean("mrql.trace.exp.execution",trace_exp_execution);
        quiet_execution = conf.getBoolean("mrql.quiet.execution",quiet_execution);
        testing = conf.getBoolean("mrql.testing",testing);
        info = conf.getBoolean("mrql.info",info);
    }

    public static ArrayList<String> extra_args = new ArrayList<String>();

    /** read configuration parameters from the Main args */
    public static Bag parse_args ( String args[], Configuration conf ) throws Exception {
        int i = 0;
        int iargs = 0;
        extra_args = new ArrayList<String>();
        ClassImporter.load_classes();
        interactive = true;
        while (i < args.length) {
            if (args[i].equals("-local")) {
                local_mode = true;
                i++;
            } else if (args[i].equals("-dist")) {
                distributed_mode = true;
                i++;
            } else if (args[i].equals("-reducers")) {
                if (++i >= args.length)
                    throw new Error("Expected number of reductions");
                nodes = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-bsp")) {
                bsp_mode = true;
                i++;
            } else if (args[i].equals("-spark")) {
                spark_mode = true;
                i++;
            } else if (args[i].equals("-bsp_tasks")) {
                if (++i >= args.length && Integer.parseInt(args[i]) < 1)
                    throw new Error("Expected max number of bsp tasks > 1");
                nodes = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-nodes")) {
                if (++i >= args.length && Integer.parseInt(args[i]) < 1)
                    throw new Error("Expected number of nodes > 1");
                nodes = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-bsp_msg_size")) {
                if (++i >= args.length && Integer.parseInt(args[i]) < 10000)
                    throw new Error("Expected max number of bsp messages before subsync() > 10000");
                bsp_msg_size = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-mapjoin_size")) {
                if (++i >= args.length)
                    throw new Error("Expected number of MBs");
                mapjoin_size = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-cache_size")) {
                if (++i >= args.length)
                    throw new Error("Expected number of entries");
                map_cache_size = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-tmp")) {
                if (++i >= args.length)
                    throw new Error("Expected a temporary directory");
                tmpDirectory = args[i];
                i++;
            } else if (args[i].equals("-bag_size")) {
                if (++i >= args.length && Integer.parseInt(args[i]) < 10000)
                    throw new Error("Expected max size of materialized bag > 10000");
                max_materialized_bag = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-bag_print")) {
                if (++i >= args.length)
                    throw new Error("Expected number of bag elements to print");
                max_bag_size_print = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-split_size")) {
                if (++i >= args.length)
                    throw new Error("Expected a split size");
                range_split_size = Long.parseLong(args[i]);
                i++;
            } else if (args[i].equals("-max_merged")) {
                if (++i >= args.length)
                    throw new Error("Expected a max number of merged streams");
                max_merged_streams = Integer.parseInt(args[i]);
                i++;
            } else if (args[i].equals("-trace")) {
                trace = true;
                i++;
            } else if (args[i].equals("-C")) {
                compile_functional_arguments = true;
                i++;
            } else if (args[i].equals("-NC")) {
                compile_functional_arguments = false;
                i++;
            } else if (args[i].equals("-P")) {
                trace_execution = true;
                i++;
            } else if (args[i].equals("-quiet")) {
                quiet_execution = true;
                i++;
            } else if (args[i].equals("-info")) {
                info = true;
                i++;
            } else if (args[i].equals("-trace_execution")) {
                trace_execution = true;
                trace_exp_execution = true;
                compile_functional_arguments = false;
                i++;
            } else if (args[i].equals("-methods")) {
                System.out.print("\nImported methods: ");
                ClassImporter.print_methods();
                System.out.println();
                System.out.print("\nAggregations:");
                Translator.print_aggregates();
                System.out.println();
                i++;
            } else if (args[i].charAt(0) == '-')
                throw new Error("Unknown MRQL parameter: "+args[i]);
            else {
                if (interactive) {
                    Main.query_file = args[i++];
                    interactive = false;
                } else extra_args.add(args[i++]);
            }
        };
        if (hadoop_mode)
            write(conf);
        Plan.conf = conf;
        Bag b = new Bag();
        for ( String s: extra_args )
            b.add(new MR_string(s));
        Interpreter.new_global_binding(new VariableLeaf("args").value(),b);
        return b;
    }
}
