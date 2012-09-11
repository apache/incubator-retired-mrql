/********************************************************************************
   Copyright 2011-2012 Leonidas Fegaras, University of Texas at Arlington

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   File: Main.java
   The MRQL main program
   Programmer: Leonidas Fegaras, UTA
   Date: 10/14/10 - 08/25/12

********************************************************************************/

package hadoop.mrql;

import java_cup.runtime.*;
import java.util.ArrayList;
import java.io.*;
import Gen.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.Options;
import jline.*;


final class Config {
    public final static String version = "0.8.2";
    public static boolean loaded = false;

    // true for Hadoop, false for plain Java
    public static boolean hadoop_mode = true;
    // true for local Hadoop mode
    public static boolean local_hadoop_mode = true;
    // true, for BSP mode using Hama
    public static boolean bsp_mode = false;
    // if true, it process the input interactively
    public static boolean interactive = true;
    // compile the MR functional arguments to Java bytecode at run-time
    // (each task-tracker repeats the compilation at the MR setup time)
    public static boolean compile_functional_arguments = false;
    // if true, generates info about all compilation and optimization steps
    public static boolean trace = false;
    // maximum number of BSP tasks
    public static int bsp_tasks = 2;
    // number of reduced tasks
    public static int reduce_tasks = -1;
    // max distributed cache size for MapJoin (fragment-replicate join) in MBs
    public static int mapjoin_size = 50;
    // max entries for in-mapper combiner before they are flushed out
    public static int map_cache_size = 100000;
    // max number of bag elements to print
    public static int max_bag_size_print = 20;
    // max size of materialized vector before is spilled to a file:
    public static int max_materialized_bag = 500000;
    // max number of incoming messages before a sub-sync()
    public static int bsp_msg_size = 100000;
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

    static void write ( Configuration conf ) {
	conf.setBoolean("mrql.hadoop.mode",hadoop_mode);
	conf.setBoolean("mrql.hadoop.local_mode",local_hadoop_mode);
	conf.setBoolean("mrql.bsp.mode",bsp_mode);
	conf.setBoolean("mrql.interactive",interactive);
	conf.setBoolean("mrql.compile.functional.arguments",compile_functional_arguments);
	conf.setBoolean("mrql.trace",trace);
	conf.setInt("mrql.bsp.tasks",bsp_tasks);
	conf.setInt("mrql.reduce.tasks",reduce_tasks);
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
    }

    static void read ( Configuration conf ) {
	if (loaded)
	    return;
	loaded = true;
	hadoop_mode = conf.getBoolean("mrql.hadoop.mode",hadoop_mode);
	local_hadoop_mode = conf.getBoolean("mrql.hadoop.local_mode",local_hadoop_mode);
	bsp_mode = conf.getBoolean("mrql.bsp.mode",bsp_mode);
	interactive = conf.getBoolean("mrql.interactive",interactive);
	compile_functional_arguments = conf.getBoolean("mrql.compile.functional.arguments",compile_functional_arguments);
	trace = conf.getBoolean("mrql.trace",trace);
	bsp_tasks = conf.getInt("mrql.bsp.tasks",bsp_tasks);
	reduce_tasks = conf.getInt("mrql.reduce.tasks",reduce_tasks);
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
    }

    public static Bag parse_args ( String args[], Configuration conf ) throws Exception {
	int i = 0;
	int iargs = 0;
	ArrayList<String> extra_args = new ArrayList<String>();
	ClassImporter.load_classes();
	hadoop_mode = false;
	interactive = true;
	Main.parser.scanner = null;
	while (i < args.length) {
	    if (args[i].equals("-hadoop")) {
		hadoop_mode = true;
		i++;
	    } else if (args[i].equals("-bsp")) {
		bsp_mode = true;
		i++;
	    } else if (args[i].equals("-local")) {
		hadoop_mode = true;
		local_hadoop_mode = true;
		i++;
	    } else if (args[i].equals("-dist")) {
		hadoop_mode = true;
		local_hadoop_mode = false;
		i++;
	    } else if (args[i].equals("-reducers")) {
		if (++i >= args.length)
		    throw new Error("Expected number of reductions");
		reduce_tasks = Integer.parseInt(args[i]);
		i++;
	    } else if (args[i].equals("-bsp_tasks")) {
		if (++i >= args.length && Integer.parseInt(args[i]) < 1)
		    throw new Error("Expected max number of bsp tasks > 1");
		bsp_tasks = Integer.parseInt(args[i]);
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
	    } else if (args[i].equals("-P")) {
		trace_execution = true;
		i++;
	    } else if (args[i].equals("-trace_execution")) {
		trace_execution = true;
		trace_exp_execution = true;
		i++;
	    } else if (args[i].equals("-methods")) {
		System.out.print("\nImported methods: ");
		ClassImporter.print_methods();
		System.out.println();
		System.out.print("\nAggregations:");
		Translate.print_aggregates();
		System.out.println();
		i++;
	    } else if (args[i].charAt(0) == '-')
		throw new Error("Unknown MRQL parameter: "+args[i]);
	    else {
		if (interactive) {
		    try {
			Main.parser.scanner = new MRQLLex(new FileInputStream(args[i]));
		    } catch (Exception e) {
			Path path = new Path(args[i]);
			FileSystem fs = path.getFileSystem(conf);
			Main.parser.scanner = new MRQLLex(fs.open(path));
		    };
		    i++;
		    Main.parser.setScanner(Main.parser.scanner);
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
	Interpreter.global_binding(new VariableLeaf("args").value(),b);
	return b;
    }
}


public class Main extends Configured implements Tool {
    public static PrintStream print_stream;
    public static Configuration conf;
    static MRQLParser parser = new MRQLParser();

    public static void include_file ( String file ) {
	try {
	    MRQLParser old_parser = parser;
	    parser = new MRQLParser();
	    boolean old_interactive = Config.interactive;
	    try {
		parser.scanner = new MRQLLex(new FileInputStream(file));
	    } catch (Exception e) {
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
		Main.parser.scanner = new MRQLLex(fs.open(path));
	    };
	    parser.setScanner(parser.scanner);
	    Config.interactive = false;
	    parser.parse();
	    Config.interactive = old_interactive;
	    parser = old_parser;
	} catch (Exception ex) {
	    ex.printStackTrace(System.err);
	    throw new Error(ex);
	}
    }

    public int run ( String args[] ) throws Exception {
	Config.parse_args(args,conf);
	Evaluator.init(conf);
	System.out.print("MRQL version "+Config.version+" (");
	if (Config.compile_functional_arguments)
	    System.out.print("compiled ");
	if (Config.hadoop_mode)
	    if (Config.bsp_mode)
		System.out.println("Hama mode over "+Config.bsp_tasks+" BSP tasks)");
	    else System.out.println("Hadoop mode)");
	else if (Config.bsp_mode)
	    System.out.println("in-memory BSP mode)");
	else System.out.println("in-memory Map-Reduce mode)");
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
		    if (Config.hadoop_mode && Config.bsp_mode)
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
		    parser.scanner = new MRQLLex(new StringReader(line));
		    MRQLLex.reset();
		    parser.setScanner(parser.scanner);
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
		if (Config.hadoop_mode)
		    Plan.clean();
		if (Config.compile_functional_arguments)
		    Compiler.clean();
	    }
	} else try {
		if (Config.hadoop_mode && Config.bsp_mode)
		    Config.write(Plan.conf);
		parser.parse();
	    } finally {
		if (Config.hadoop_mode)
		    Plan.clean();
		if (Config.compile_functional_arguments)
		    Compiler.clean();
	    };
	return 0;
    }

    public static void main ( String[] args ) throws Exception {
	boolean hadoop = false;
	for ( String arg: args )
	    hadoop |= arg.equals("-local") || arg.equals("-dist") || arg.equals("-hadoop");
	if (!hadoop)
	    new Main().run(args);
	else {
	    conf = Evaluator.new_configuration();
	    GenericOptionsParser gop = new GenericOptionsParser(conf,args);
	    conf = gop.getConfiguration();
	    args = gop.getRemainingArgs();
	    ToolRunner.run(conf,new Main(),args);
	}
    }
}
