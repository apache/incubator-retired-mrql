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

   File: Hama.java
   The Hama BSP physical operators for MRQL
   Programmer: Leonidas Fegaras, UTA
   Date: 06/01/12 - 08/20/12

********************************************************************************/

package hadoop.mrql;

import Gen.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hama.bsp.*;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.HamaConfiguration;
import org.apache.hadoop.conf.Configuration;


class BSPPlan extends Plan {
    final static class BSPop extends BSP<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> {
	static BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> this_peer;
	final static MRContainer null_key = new MRContainer(new MR_byte(0));
	// special message for sub-sync()
	final static MRData more_to_come = new MR_sync();
	final static MRData more_supersteps = new MR_more_bsp_steps();

	private Function superstep_fnc;      // superstep function
	private MRData state;                // BSP state
	private boolean orderp;              // will output be ordered?
	private MRData source;               // BSP input
	private Function acc_fnc;            // aggregator
	private MRData acc_result;           // aggregation result
	private static String[] all_peers;   // all BSP peers
	// a master peer that coordinates and collects results of partial aggregations
	private String masterTask;
	// buffer for received messages -- regularly in a vector, but can be spilled in a local file
	Bag msg_cache;
	int sc = 1;

	private static String shuffle ( MRData key ) {
	    return all_peers[Math.abs(key.hashCode()) % all_peers.length];
	}

	// to exit a BSP loop, all peers must agree to exit (this is used in BSPTranslate.bspSimplify)
	public static MR_bool synchronize ( MR_bool mr_exit ) {
	    if (!Config.hadoop_mode)
		return mr_exit;
	    // shortcut: if we know for sure that all peers want to exit/continue, we don't have to poll
	    if (mr_exit == SystemFunctions.bsp_true_value          // must be ==, not equals
		|| mr_exit == SystemFunctions.bsp_false_value)
		return mr_exit;
	    try {
		// this case is only used for checking the exit condition of repeat/closure
		boolean exit = mr_exit.get();
		if (!exit)
		    // this peer is not ready to exit, so no peer should exit
		    for ( String p: this_peer.getAllPeerNames() )
			this_peer.send(p,new MRContainer(more_supersteps));
		this_peer.sync();
		// now exit is true if no peer sent a "more_supersteps" message
		exit = this_peer.getNumCurrentMessages() == 0;
		this_peer.clear();
		return (exit) ? SystemFunctions.bsp_true_value : SystemFunctions.bsp_false_value;
	    } catch (Exception ex) {
		throw new Error(ex);
	    }
	}

	private Bag readLocalSnapshot ( final BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer ) {
	    return new Bag(new BagIterator() {
		    final MRContainer key = new MRContainer();
		    final MRContainer value = new MRContainer();
		    public boolean hasNext () {
			try {
			    return peer.readNext(key,value);
			} catch (IOException e) {
			    throw new Error(e);
			}
		    }
		    public MRData next () {
			return value.data();
		    }
		});
	}

	private void writeLocalSnapshot ( Bag snapshot,
					  BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
	        throws IOException {
	    final MRContainer key = new MRContainer();
	    final MRContainer value = new MRContainer();
	    for ( MRData v: snapshot ) {
		Tuple t = (Tuple)v;
		if (orderp) {       // prepare for sorting
		    key.set(t.get(2));
		    value.set(t.get(1));
		    peer.write(key,value);
		} else {
		    value.set(t.get(1));
		    peer.write(null_key,value);
		}
	    }
	}

	// receive messages from other peers
	private void receive_messages ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
	        throws IOException, SyncException, InterruptedException {
	    boolean expect_more = false;   // are we expecting more incoming messages?
	    do {
		// just in case this peer did a regular-sync() before the others did a sub-sync()
		expect_more = false;
		MRContainer msg;
		// cache the received messages
		while ((msg = peer.getCurrentMessage()) != null)
		    // if at least one peer sends a more_to_come message, then expect_more
		    if (msg.data().equals(more_to_come))
			expect_more = true;
		    else msg_cache.add(msg.data());
		if (expect_more)
		    peer.sync();   // sub-sync()
	    } while (expect_more);
	}

	// send the messages produced by a superstep to peers and then receive the replies
	private void send_messages ( Bag msgs,
				     BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
	        throws IOException, SyncException, InterruptedException {
	    int size = 0;
	    msg_cache.clear();
	    for ( MRData m: msgs ) {
		Tuple t = (Tuple)m;
		// if there are too many messages to send, then sub-sync()
		if ( size++ > Config.bsp_msg_size ) {
		    // tell all peers that there is more to come after sync
		    for ( String p: all_peers )
			if (!peer.getPeerName().equals(p))
			    peer.send(p,new MRContainer(more_to_come));
		    peer.sync();  // sub-sync()
		    size = 0;
		    MRContainer msg;
		    // cache the received messages
		    while ((msg = peer.getCurrentMessage()) != null)
			if (!msg.data().equals(more_to_come))
			    msg_cache.add(msg.data());
		};
		// suffle messages based on key
		peer.send(shuffle(t.get(0)),new MRContainer(t.get(1)));
	    };
	    peer.sync();   // regular-sync()
	    receive_messages(peer);
	}

	@Override
	public void bsp ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
	       throws IOException, SyncException, InterruptedException {
	    final Tuple triple = new Tuple(3);
	    MRData snapshot = readLocalSnapshot(peer);
	    Tuple result;
	    boolean skip = false;
	    String tabs = "";
	    int step = 0;
	    boolean exit;
	    do {
		if (!skip)
		    step++;
		if (!skip && Config.trace_execution) {
		    tabs = Interpreter.tabs(Interpreter.tab_count);
		    System.err.println(tabs+"  Superstep "+step+" ["+peer.getPeerName()+"]:");
		    System.err.println(tabs+"      messages ["+peer.getPeerName()+"]: "+msg_cache);
		    System.err.println(tabs+"      snapshot ["+peer.getPeerName()+"]: "+snapshot);
		    System.err.println(tabs+"      state ["+peer.getPeerName()+"]: "+state);
		};
		triple.set(0,msg_cache);
		triple.set(1,snapshot);
		triple.set(2,state);
		this_peer = peer;
		// evaluate one superstep
		result = (Tuple)superstep_fnc.eval(triple);
		Bag msgs = (Bag)result.get(0);
		snapshot = result.get(1);
		exit = ((MR_bool)result.get(3)).get();
		state = result.get(2);
		// shortcuts: if we know for sure that all peers want to exit/continue
		if (result.get(3) == SystemFunctions.bsp_true_value) {  // must be ==, not equals
		    peer.sync();
		    if (Config.trace_execution)
			System.err.println(tabs+"      result ["+peer.getPeerName()+"]: "+result);
		    break;
		};
		if (result.get(3) == SystemFunctions.bsp_false_value) {
		    if (Config.trace_execution)
			System.err.println(tabs+"      result ["+peer.getPeerName()+"]: "+result);
		    send_messages(msgs,peer);
		    skip = false;
		    continue;
		};
		// shortcut: skip is true when NONE of the peers sent any messages
		skip = (msgs == SystemFunctions.bsp_empty_bag);  // must be ==, not equals
		if (skip)
		    continue;
		if (Config.trace_execution)
		    System.err.println(tabs+"      result ["+peer.getPeerName()+"]: "+result);
		exit = synchronize((MR_bool)result.get(3)).get();
		send_messages(msgs,peer);
	    } while (!exit);
	    if (acc_result == null) {
		// the BSP result is a bag that needs to be dumped to the HDFS
		writeLocalSnapshot((Bag)snapshot,peer);
	    } else {
		// the BSP result is an aggregation:
		//     send the partial results to the master peer
		peer.send(masterTask,new MRContainer(snapshot));
		peer.sync();
		if (peer.getPeerName().equals(masterTask)) {
		    // only the master peer collects the partial aggregations
		    MRContainer msg;
		    while ((msg = peer.getCurrentMessage()) != null)
			acc_result = acc_fnc.eval(new Tuple(acc_result,msg.data()));
		    // write the final aggregation result
		    peer.write(null_key,new MRContainer(acc_result));
	       }
	    }
	}

	@Override
	public void setup ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer ) {
	    try {
		super.setup(peer);
		Configuration conf = peer.getConfiguration();
		Config.read(conf);
		if (Plan.conf == null)
		    Plan.conf = conf;
		this_peer = peer;
		all_peers = peer.getAllPeerNames();
		Tree code = Tree.parse(conf.get("mrql.superstep"));
		superstep_fnc = functional_argument(conf,code);
		code = Tree.parse(conf.get("mrql.initial.state"));
		state = Interpreter.evalE(code);
		if (conf.get("mrql.zero") != null && !conf.get("mrql.zero").equals("")) {
		    code = Tree.parse(conf.get("mrql.zero"));
		    acc_result = Interpreter.evalE(code);
		    code = Tree.parse(conf.get("mrql.accumulator"));
		    acc_fnc = functional_argument(conf,code);
		} else acc_result = null;
		orderp = conf.getBoolean("mrql.orderp",false);
		masterTask = all_peers[peer.getNumPeers()/2];
		msg_cache = new Bag(1000);
	    } catch (Exception e) {
		e.printStackTrace();
		throw new Error("Cannot setup the Hama BSP job: "+e);
	    }
	}
    }

    // set Hama's min split size and number of BSP tasks
    public static void setupSplits ( BSPJob job, DataSet ds ) throws IOException {
	long[] sizes = new long[ds.source.size()];
	if (sizes.length > Config.bsp_tasks)
	    throw new Error("Cannot distribute "+sizes.length+" files over "+Config.bsp_tasks+" BSP tasks");
	for ( int i = 0; i < sizes.length; i++ )
	    sizes[i] = ds.source.get(i).size(Plan.conf);
	long total_size = 0;
	for ( long size: sizes )
	    total_size += size;
	long split_size = Math.max(total_size/Config.bsp_tasks,100000);
	int tasks = 0;
	do {  // adjust split_size
	    tasks = 0;
	    for ( long size: sizes )
		tasks += (int)Math.ceil(size/(double)split_size);
	    if (tasks > Config.bsp_tasks)
		split_size = (long)Math.ceil((double)split_size*1.01);
	} while (tasks > Config.bsp_tasks);
	job.setNumBspTask(tasks);
	System.err.println("*** Using "+tasks+" BSP tasks (out of a max "+Config.bsp_tasks+")."
			   +" Each task will handle about "+Math.min(total_size/Config.bsp_tasks,split_size)
			   +" bytes of input data.");
	job.set("bsp.min.split.size",Long.toString(split_size));
    }

    public final static DataSet BSP ( int source_num,   // output tag
				      Tree superstep,   // superstep function
				      Tree init_state,  // initial state
				      boolean orderp,   // do we need to order the result?
				      DataSet source    // input dataset
				      ) throws Exception {
	String newpath = new_path(conf);
	conf.set("mrql.superstep",superstep.toString());
	conf.set("mrql.initial.state",init_state.toString());
	conf.set("mrql.zero","");
	conf.setBoolean("mrql.orderp",orderp);
	BSPJob job = new BSPJob((HamaConfiguration)conf,BSPop.class);
	setupSplits(job,source);
	job.setJobName(newpath);
	distribute_compiled_arguments(job.getConf());
	job.setBspClass(BSPop.class);
	Path outpath = new Path(newpath);
	job.setOutputPath(outpath);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	job.setInputFormat(MultipleBSPInput.class);
	FileInputFormat.setInputPaths(job,source.merge());
	job.waitForCompletion(true);
	DataSource s = new BinaryDataSource(source_num,newpath,conf);
	s.to_be_merged = orderp;
	return new DataSet(s,0,3);
    }

    public final static MRData BSPaggregate ( Tree superstep,   // superstep function
					      Tree init_state,  // initial state
					      Tree acc_fnc,     // accumulator function
					      Tree zero,        // zero value for the accumulator
					      DataSet source    // input dataset
					      ) throws Exception {
	String newpath = new_path(conf);
	conf.set("mrql.superstep",superstep.toString());
	conf.set("mrql.initial.state",init_state.toString());
	conf.set("mrql.accumulator",acc_fnc.toString());
	conf.set("mrql.zero",zero.toString());
	conf.setBoolean("mrql.orderp",false);
	BSPJob job = new BSPJob((HamaConfiguration)conf,BSPop.class);
	setupSplits(job,source);
	job.setJobName(newpath);
	distribute_compiled_arguments(job.getConf());
	job.setBspClass(BSPop.class);
	Path outpath = new Path(newpath);
	job.setOutputPath(outpath);
	job.setOutputKeyClass(MRContainer.class);
	job.setOutputValueClass(MRContainer.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	job.setInputFormat(MultipleBSPInput.class);
	FileInputFormat.setInputPaths(job,source.merge());
	job.waitForCompletion(true);
	FileSystem fs = outpath.getFileSystem(conf);
	FileStatus[] files = fs.listStatus(outpath);
	for ( int i = 0; i < files.length; i++ )
	    if (files[i].getLen() > 74) {
		SequenceFile.Reader sreader = new SequenceFile.Reader(fs,files[i].getPath(),conf);
		MRContainer key = new MRContainer();
		MRContainer value = new MRContainer();
		sreader.next(key,value);
		sreader.close();
		return value.data();
	    };
	return null;
    }
 }
