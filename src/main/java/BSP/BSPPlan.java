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
import java.io.*;
import java.util.Arrays;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hama.bsp.*;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.HamaConfiguration;
import org.apache.hadoop.conf.Configuration;


/** Evaluate a BSP plan using Hama */
final public class BSPPlan extends Plan {

    final static Configuration getConfiguration ( BSPJob job ) {
        return job.getConf();   // use job.getConfiguration() for Hama 0.6.0
    }

    /** The BSP evaluator */
    final static class BSPop extends BSP<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> {
        final static MRContainer null_key = new MRContainer(new MR_byte(0));
        // special message for sub-sync()
        final static MRData more_to_come = new MR_sync();
        final static MRData more_supersteps = new MR_more_bsp_steps();

        private int source_num;
        private Function superstep_fnc;      // superstep function
        private MRData state;                // BSP state
        private boolean orderp;              // will output be ordered?
        private MRData source;               // BSP input
        private Function acc_fnc;            // aggregator
        private MRData acc_result;           // aggregation result
        private static String[] all_peer_names;   // all BSP peer names
        private static BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer>[] all_peers;   // all BSP peers
        // a master peer that coordinates and collects results of partial aggregations
        private String masterTask;
        // buffer for received messages -- regularly in a vector, but can be spilled in a local file
        Bag msg_cache;
        // the cache that holds all local data in memory
        Tuple local_cache;

        private static BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> getPeer ( String name ) {
            for ( int i = 0; i < all_peer_names.length; i++ )
                if (all_peer_names[i].equals(name))
                    return all_peers[i];
            throw new Error("Unknown peer: "+name);
        }

        private static void setPeer ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer ) {
            String name = peer.getPeerName();
            for ( int i = 0; i < all_peer_names.length; i++ )
                if (all_peer_names[i].equals(name))
                    all_peers[i] = peer;
        }

        /** shuffle values to BSP peers based on uniform hashing on key */
        private static String shuffle ( MRData key ) {
            return all_peer_names[Math.abs(key.hashCode()) % all_peer_names.length];
        }

        /** to exit a BSP loop, all peers must agree to exit (this is used in BSPTranslate.bspSimplify) */
        public static MR_bool synchronize ( MR_string peerName, MR_bool mr_exit ) {
            return synchronize(getPeer(peerName.get()),mr_exit);
        }

        /** to exit a BSP loop, all peers must agree to exit (this is used in BSPTranslate.bspSimplify) */
        public static MR_bool synchronize ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer, MR_bool mr_exit ) {
            if (!Config.hadoop_mode)
                return mr_exit;
            // shortcut: if we know for sure that all peers want to exit/continue, we don't have to poll
            if (mr_exit == SystemFunctions.bsp_true_value          // must be ==, not equals
                || mr_exit == SystemFunctions.bsp_false_value)
                return mr_exit;
            try {
                // this case is only used for checking the exit condition of repeat/closure
                boolean exit = mr_exit.get();
                if (all_peer_names.length <= 1)
                    return (exit) ? SystemFunctions.bsp_true_value : SystemFunctions.bsp_false_value;
                if (!exit)
                    // this peer is not ready to exit, so no peer should exit
                    for ( String p: peer.getAllPeerNames() )
                        peer.send(p,new MRContainer(more_supersteps));
                peer.sync();
                // now exit is true if no peer sent a "more_supersteps" message
                exit = peer.getNumCurrentMessages() == 0;
                peer.clear();
                return (exit) ? SystemFunctions.bsp_true_value : SystemFunctions.bsp_false_value;
            } catch (Exception ex) {
                throw new Error(ex);
            }
        }

        /** collect a bag from all peers by distributing the local copy s */
        public static Bag distribute ( MR_string peerName, Bag s ) {
            BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer = getPeer(peerName.get());
            if (!Config.hadoop_mode)
                return s;
            try {
                for ( MRData e: s )
                    for ( String p: all_peer_names )
                        peer.send(p,new MRContainer(e));
                peer.sync();
                MRContainer msg;
                Bag res = new Bag();
                while ((msg = peer.getCurrentMessage()) != null)
                    if (!res.contains(msg.data()))
                        res.add(msg.data());
                peer.clear();
                return res;
            } catch (Exception ex) {
                throw new Error(ex);
            }
        }

        private void readLocalSources ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
                throws IOException {
            MRContainer key = new MRContainer();
            MRContainer value = new MRContainer();
            while (peer.readNext(key,value)) {
                Tuple p = (Tuple)(value.data());
                ((Bag)local_cache.get(((MR_int)p.first()).get())).add(p.second());
            }
        }

        private void writeLocalResult ( Bag result,
                                        BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
                throws IOException {
            MRContainer key = new MRContainer();
            MRContainer value = new MRContainer();
            for ( MRData v: result )
                if (orderp) {       // prepare for sorting
                    Tuple t = (Tuple)v;
                    key.set(t.get(1));
                    value.set(t.get(0));
                    peer.write(key,value);
                } else {
                    value.set(v);
                    peer.write(null_key,value);
                }
        }

        /** receive messages from other peers */
        private void receive_messages ( final BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
                throws IOException, SyncException, InterruptedException {
            if (Config.bsp_msg_size <= 0) {    // no buffering
                msg_cache = new Bag(new BagIterator() {
                        MRContainer msg;
                        public boolean hasNext () {
                            try {
                                return (msg = peer.getCurrentMessage()) != null;
                            } catch (Exception ex) {
                                throw new Error(ex);
                            }
                        } 
                        public MRData next () {
                            return msg.data();
                        }
                    });
            } else {
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
        }

        /** send the messages produced by a superstep to peers and then receive the replies */
        private void send_messages ( Bag msgs,
                                     BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
                throws IOException, SyncException, InterruptedException {
            int size = 0;
            if (Config.bsp_msg_size > 0)
                msg_cache.clear();
            for ( MRData m: msgs ) {
                Tuple t = (Tuple)m;
                // if there are too many messages to send, then sub-sync()
                if ( Config.bsp_msg_size > 0 && size++ > Config.bsp_msg_size ) {
                    // tell all peers that there is more to come after sync
                    for ( String p: all_peer_names )
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
                // suffle messages based on key; needs new MRContainer object
                peer.send(shuffle(t.get(0)),new MRContainer(t.get(1)));
            };
            peer.sync();   // regular-sync()
            receive_messages(peer);
        }

        @Override
        public void bsp ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer )
               throws IOException, SyncException, InterruptedException {
            final Tuple stepin = new Tuple(4);
            stepin.set(3,new MR_string(peer.getPeerName()));
            Tuple result;
            boolean skip = false;
            String tabs = "";
            int step = 0;
            boolean exit;
            if (Evaluator.evaluator == null)
                try {
                    Evaluator.evaluator = (Evaluator)Class.forName("org.apache.mrql.BSPEvaluator").newInstance();
                } catch (Exception ex) {
                    throw new Error(ex);
                };
            readLocalSources(peer);
            setPeer(peer);
            do {
                if (!skip)
                    step++;
                if (!skip && Config.trace_execution) {
                    tabs = Interpreter.tabs(Interpreter.tab_count);
                    System.err.println(tabs+"  Superstep "+step+" ["+peer.getPeerName()+"]:");
                    System.err.println(tabs+"      messages ["+peer.getPeerName()+"]: "+msg_cache);
                    System.err.println(tabs+"      state ["+peer.getPeerName()+"]: "+state);
                    for ( int i = 0; i < local_cache.size(); i++)
                        if (local_cache.get(i) instanceof Bag && ((Bag)local_cache.get(i)).size() > 0)
                            System.out.println(tabs+"      cache ["+peer.getPeerName()+"] "+i+": "+local_cache.get(i));
                };
                stepin.set(0,local_cache);
                stepin.set(1,msg_cache);
                stepin.set(2,state);
                // evaluate one superstep
                result = (Tuple)superstep_fnc.eval(stepin);
                Bag msgs = (Bag)result.get(0);
                exit = ((MR_bool)result.get(2)).get();
                state = result.get(1);
                // shortcuts: if we know for sure that all peers want to exit/continue
                if (result.get(2) == SystemFunctions.bsp_true_value) {  // must be ==, not equals
                    peer.sync();
                    if (Config.trace_execution)
                        System.err.println(tabs+"      result ["+peer.getPeerName()+"]: "+result);
                    break;
                };
                if (result.get(2) == SystemFunctions.bsp_false_value) {
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
                exit = synchronize(peer,(MR_bool)result.get(2)).get();
                send_messages(msgs,peer);
            } while (!exit);
            if (acc_result == null) {
                // the BSP result is a bag that needs to be dumped to the HDFS
                writeLocalResult((Bag)(local_cache.get(source_num)),peer);
                // if there more results, dump them to HDFS
                final MR_long key = new MR_long(0);
                final MRContainer key_container = new MRContainer(key);
                final MRContainer data_container = new MRContainer(new MR_int(0));
                int loc = 0;
                while ( loc < all_peer_names.length && peer.getPeerName().equals(all_peer_names[loc]) )
                    loc++;
                Configuration conf = peer.getConfiguration();
                String[] out_paths = conf.get("mrql.output.paths").split(",");
                for ( int i = 1; i < out_paths.length; i++ ) {
                    String[] s = out_paths[i].split(":");
                    int out_num = Integer.parseInt(s[0]);
                    Path path = new Path(s[1]+"/peer"+loc);
                    FileSystem fs = path.getFileSystem(conf);
                    SequenceFile.Writer writer
                        = new SequenceFile.Writer(fs,conf,path,
                                                  MRContainer.class,MRContainer.class);
                    long count = 0;
                    for ( MRData e: (Bag)(local_cache.get(out_num)) ) {
                        key.set(count++);
                        data_container.set(e);
                        writer.append(key_container,data_container);
                    };
                    writer.close();
                };
            } else {
                // the BSP result is an aggregation:
                //     send the partial results to the master peer
                peer.send(masterTask,new MRContainer(local_cache.get(source_num)));
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
        @SuppressWarnings("unchecked")
        public void setup ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer ) {
            try {
                super.setup(peer);
                Configuration conf = peer.getConfiguration();
                Config.read(conf);
                if (Plan.conf == null)
                    Plan.conf = conf;
                all_peer_names = peer.getAllPeerNames();
                all_peers = new BSPPeerImpl[all_peer_names.length];
                Arrays.sort(all_peer_names);  // is this necessary?
                source_num = conf.getInt("mrql.output.tag",0);
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
                masterTask = all_peer_names[peer.getNumPeers()/2];
                msg_cache = new Bag(1000);
                local_cache = new Tuple(max_input_files);
                for ( int i = 0; i < max_input_files; i++ )
                    local_cache.set(i,new Bag());
            } catch (Exception e) {
                e.printStackTrace();
                throw new Error("Cannot setup the Hama BSP job: "+e);
            }
        }

        @Override
        public void cleanup ( BSPPeer<MRContainer,MRContainer,MRContainer,MRContainer,MRContainer> peer ) throws IOException {
            if (!Config.local_mode)
                clean();
            local_cache = null;
            super.cleanup(peer);
        }
    }

    /** set Hama's min split size and number of BSP tasks (doesn't work with Hama 0.6.0) */
    public static void setupSplits ( BSPJob job, DataSet ds ) throws IOException {
        long[] sizes = new long[ds.source.size()];
        if (sizes.length > Config.nodes)
            throw new Error("Cannot distribute "+sizes.length+" files over "+Config.nodes+" BSP tasks");
        for ( int i = 0; i < sizes.length; i++ )
            sizes[i] = ds.source.get(i).size(Plan.conf);
        long total_size = 0;
        for ( long size: sizes )
            total_size += size;
        long split_size = Math.max(total_size/Config.nodes,100000);
        int tasks = 0;
        do {  // adjust split_size
            tasks = 0;
            for ( long size: sizes )
                tasks += (int)Math.ceil(size/(double)split_size);
            if (tasks > Config.nodes)
                split_size = (long)Math.ceil((double)split_size*1.01);
        } while (tasks > Config.nodes);
        job.setNumBspTask(tasks);
        System.err.println("*** Using "+tasks+" BSP tasks (out of a max "+Config.nodes+")."
                           +" Each task will handle about "+Math.min(total_size/Config.nodes,split_size)
                           +" bytes of input data.");
        job.set("bsp.min.split.size",Long.toString(split_size));
    }

    /** Evaluate a BSP operation that returns a DataSet
     * @param source_nums   output tags
     * @param superstep     the superstep function
     * @param init_state    initial state
     * @param orderp        do we need to order the result?
     * @param source        input dataset
     * @return a new data source that contains the result
     */
    public final static MRData BSP ( int[] source_nums, // output tags
                                     Tree superstep,    // superstep function
                                     Tree init_state,   // initial state
                                     boolean orderp,    // do we need to order the result?
                                     DataSet source     // input dataset
                                     ) throws Exception {
        String[] newpaths = new String[source_nums.length];
        newpaths[0] = new_path(conf);
        conf.set("mrql.output.paths",source_nums[0]+":"+newpaths[0]);
        for ( int i = 1; i < source_nums.length; i++ ) {
            newpaths[i] = new_path(conf);
            Path path = new Path(newpaths[1]);
            FileSystem fs = path.getFileSystem(conf);
            fs.mkdirs(path);
            conf.set("mrql.output.paths",conf.get("mrql.output.paths")+","+source_nums[i]+":"+newpaths[i]);
        };
        conf.set("mrql.superstep",superstep.toString());
        conf.set("mrql.initial.state",init_state.toString());
        conf.set("mrql.zero","");
        conf.setInt("mrql.output.tag",source_nums[0]);
        conf.setBoolean("mrql.orderp",orderp);
        BSPJob job = new BSPJob((HamaConfiguration)conf,BSPop.class);
        setupSplits(job,source);
        job.setJobName(newpaths[0]);
        distribute_compiled_arguments(getConfiguration(job));
        job.setBspClass(BSPop.class);
        Path outpath = new Path(newpaths[0]);
        job.setOutputPath(outpath);
        job.setOutputKeyClass(MRContainer.class);
        job.setOutputValueClass(MRContainer.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        job.setInputFormat(MultipleBSPInput.class);
        FileInputFormat.setInputPaths(job,source.merge());
        job.waitForCompletion(true);
        if (source_nums.length == 1) {
            BinaryDataSource ds = new BinaryDataSource(source_nums[0],newpaths[0],conf);
            ds.to_be_merged = orderp;
            return new MR_dataset(new DataSet(ds,0,3));
        } else {
            MRData[] s = new MRData[source_nums.length];
            for ( int i = 0; i < source_nums.length; i++ ) {
                BinaryDataSource ds = new BinaryDataSource(source_nums[i],newpaths[i],conf);
                ds.to_be_merged = orderp;
                s[i] = new MR_dataset(new DataSet(ds,0,3));
            };
            return new Tuple(s);
        }
    }

    /** Evaluate a BSP operation that aggregates the results
     * @param source_num    output tag
     * @param superstep     the superstep function
     * @param init_state    initial state
     * @param acc_fnc       accumulator function
     * @param zero          zero value for the accumulator
     * @param source        input dataset
     * @return the aggregation result
     */
    public final static MRData BSPaggregate ( int source_num,   // output tag
                                              Tree superstep,   // superstep function
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
        conf.setInt("mrql.output.tag",source_num);
        conf.setBoolean("mrql.orderp",false);
        BSPJob job = new BSPJob((HamaConfiguration)conf,BSPop.class);
        setupSplits(job,source);
        job.setJobName(newpath);
        distribute_compiled_arguments(getConfiguration(job));
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
        for ( int i = 0; i < files.length; i++ ) {
            SequenceFile.Reader sreader = new SequenceFile.Reader(fs,files[i].getPath(),conf);
            MRContainer key = new MRContainer();
            MRContainer value = new MRContainer();
            sreader.next(key,value);
            sreader.close();
            if (value.data() != null)
                return value.data();
        };
        return null;
    }
 }
