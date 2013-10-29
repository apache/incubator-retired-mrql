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
import java.util.Random;
import java.util.ArrayList;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.Sorter;


/** A physical plan (a superclass for both MapReduce, BSP, and Spark plans) */
public class Plan {
    public static Configuration conf;
    static ArrayList<String> temporary_paths = new ArrayList<String>();
    private static Random random_generator = new Random();
    final static int max_input_files = 100;

    /** generate a new path name in HDFS to store intermediate results */
    public static String new_path ( Configuration conf ) throws IOException {
        String dir = (Config.local_mode)
                     ? ((Config.tmpDirectory == null) ? "/tmp/mrql" : Config.tmpDirectory)
                     : "mrql";
        Path p;
        do {
            p = new Path(dir+"/mrql"+random_generator.nextInt(1000000));
        } while (p.getFileSystem(conf).exists(p));
        String path = p.toString();
        temporary_paths.add(path);
        DataSource.dataSourceDirectory.distribute(conf);
        return path;
    }

    /** remove all temporary files */
    public static void clean () throws IOException {
        for (String p: temporary_paths)
            try {
                Path path = new Path(p);
                path.getFileSystem(conf).delete(path,true);
            } catch (Exception ex) {
                FileSystem.getLocal(conf).delete(new Path(p),true);
            };
        temporary_paths.clear();
        DataSource.dataSourceDirectory.clear();
    }

    /** return the data set size in bytes */
    public final static long size ( DataSet s ) {
        return s.size(conf);
    }

    /** the cache that holds all local data in memory */
    static Tuple cache;

    /** return the cache element at location loc */
    public static synchronized MRData getCache ( int loc ) {
        return cache.get(loc);
    }

    /** set the cache element at location loc to value and return ret */
    public static synchronized MRData setCache ( int loc, MRData value, MRData ret ) {
        if (value instanceof Bag)
            ((Bag)value).materialize();
        cache.set(loc,value);
        return ret;
    }

    /** put the jar file that contains the compiled MR functional parameters into the TaskTracker classpath */
    final static void distribute_compiled_arguments ( Configuration conf ) {
        try {
            if (!Config.compile_functional_arguments)
                return;
            Path local_path = new Path("file://"+Compiler.jar_path);
            if (Config.spark_mode)
                conf.set("mrql.jar.path",local_path.toString());
            else {
                // distribute the jar file with the compiled arguments to all clients
                Path hdfs_path = new Path("mrql-tmp/class"+random_generator.nextInt(1000000)+".jar");
                FileSystem fs = hdfs_path.getFileSystem(conf);
                fs.copyFromLocalFile(false,true,local_path,hdfs_path);
                temporary_paths.add(hdfs_path.toString());
                conf.set("mrql.jar.path",hdfs_path.toString());
            }
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    /** retrieve the compiled functional argument of code */
    final static Function functional_argument ( Configuration conf, Tree code ) {
        Node n = (Node)code;
        if (n.name().equals("compiled"))
            try {
                // if the clent has not received the jar file with the compiled arguments, copy the file from HDFS
                if (Compiler.jar_path == null) {
                    Path hdfs_path = new Path(conf.get("mrql.jar.path"));
                    String local_path = Compiler.tmp_dir+"/mrql_args_"+random_generator.nextInt(1000000)+".jar";
                    FileSystem fs = hdfs_path.getFileSystem(conf);
                    fs.copyToLocalFile(false,hdfs_path,new Path("file://"+local_path));
                    Compiler.jar_path = local_path;
                };
                return Compiler.compiled(conf.getClassLoader(),n.children().nth(0).toString());
            } catch (Exception ex) {
                System.err.println("*** Warning: Unable to retrieve the compiled lambda: "+code);
                return ((Lambda) Interpreter.evalE(n.children().nth(1))).lambda();
            }
        else if (code.equals(Interpreter.identity_mapper))
            return new Function () {
                public MRData eval ( final MRData x ) { return new Bag(x); }
            };
        else return ((Lambda) Interpreter.evalE(code)).lambda();
    }

    /** comparator for MRData keys */
    public final static class MRContainerKeyComparator implements RawComparator<MRContainer> {
        int[] container_size;

        public MRContainerKeyComparator () {
            container_size = new int[1];
        }

        final public int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl ) {
            return MRContainer.compare(x,xs,xl,y,ys,yl,container_size);
        }

        final public int compare ( MRContainer x, MRContainer y ) {
            return x.compareTo(y);
        }
    }

    /** The source physical operator for binary files */
    public final static DataSet binarySource ( int source_num, String file ) {
        return new DataSet(new BinaryDataSource(source_num,file,conf),0,0);
    }

    /** The source physical operator for binary files */
    public final static DataSet binarySource ( String file ) {
        return new DataSet(new BinaryDataSource(-1,file,conf),0,0);
    }

    /** splits the range min..max into multiple ranges, one for each mapper */
    public final static DataSet generator ( int source_num, long min, long max, long split_length ) throws Exception {
        if (min > max)
            throw new Error("Wrong range: "+min+"..."+max);
        if (split_length < 1)
            if (Config.bsp_mode)
                split_length = (max-min)/Config.nodes+1;
            else split_length = Config.range_split_size;
        DataSet ds = new DataSet(0,0);
        long i = min;
        while (i+split_length <= max) {
            String file = new_path(conf);
            Path path = new Path(file);
            SequenceFile.Writer writer
                = SequenceFile.createWriter(path.getFileSystem(conf),conf,path,
                                            MRContainer.class,MRContainer.class,
                                            SequenceFile.CompressionType.NONE);
            writer.append(new MRContainer(new MR_long(i)),
                          new MRContainer(new Tuple(new MR_long(i),new MR_long(split_length))));
            writer.close();
            ds.source.add(new GeneratorDataSource(source_num,file,conf));
            i += split_length;
        };
        if (i <= max) {
            String file = new_path(conf);
            Path path = new Path(file);
            SequenceFile.Writer writer
                = SequenceFile.createWriter(path.getFileSystem(conf),conf,path,
                                            MRContainer.class,MRContainer.class,
                                            SequenceFile.CompressionType.NONE);
            writer.append(new MRContainer(new MR_long(i)),
                          new MRContainer(new Tuple(new MR_long(i),new MR_long(max-i+1))));
            writer.close();
            ds.source.add(new GeneratorDataSource(source_num,file,conf));
        };
        return ds;
    }

    /** splits the range min..max into multiple ranges, one for each mapper */
    public final static DataSet generator ( long min, long max, long split_length ) throws Exception {
        return generator(-1,min,max,split_length);
    }

    /** The source physical operator for parsing text files */
    public final static DataSet parsedSource ( int source_num, Class<? extends Parser> parser, String file, Trees args ) {
        return new DataSet(new ParsedDataSource(source_num,file,parser,args,conf),0,0);
    }

    /** The source physical operator for parsing text files */
    public final static DataSet parsedSource ( Class<? extends Parser> parser, String file, Trees args ) {
        return new DataSet(new ParsedDataSource(file,parser,args,conf),0,0);
    }

    /** merge the sorted files of the data source */
    public final static Bag merge ( final DataSource s ) throws Exception {
        Path path = new Path(s.path);
        final FileSystem fs = path.getFileSystem(conf);
        final FileStatus[] ds
            = fs.listStatus(path,
                            new PathFilter () {
                                public boolean accept ( Path path ) {
                                    return !path.getName().startsWith("_");
                                }
                            });
        int dl = ds.length;
        if (dl == 0)
            return new Bag();
        Path[] paths = new Path[dl];
        for ( int i = 0; i < dl; i++ )
            paths[i] = ds[i].getPath();
        if (dl > Config.max_merged_streams) {
            if (Config.trace)
                System.err.println("*** Merging "+dl+" files");
            Path out_path = new Path(new_path(conf));
            SequenceFile.Sorter sorter
                = new SequenceFile.Sorter(fs,new MRContainerKeyComparator(),
                                          MRContainer.class,MRContainer.class,conf);
            sorter.merge(paths,out_path);
            paths = new Path[1];
            paths[0] = out_path;
        };
        final int n = paths.length;
        SequenceFile.Reader[] sreaders = new SequenceFile.Reader[n];
        for ( int i = 0; i < n; i++ ) 
            sreaders[i] = new SequenceFile.Reader(fs,paths[i],conf);
        final SequenceFile.Reader[] readers = sreaders;
        final MRContainer[] keys_ = new MRContainer[n];
        final MRContainer[] values_ = new MRContainer[n];
        for ( int i = 0; i < n; i++ ) {
            keys_[i] = new MRContainer();
            values_[i] = new MRContainer();
        };
        return new Bag(new BagIterator () {
                int min = 0;
                boolean first = true;
                final MRContainer[] keys = keys_;
                final MRContainer[] values = values_;
                final MRContainer key = new MRContainer();
                final MRContainer value = new MRContainer();
                public boolean hasNext () {
                    if (first)
                        try {
                            first = false;
                            for ( int i = 0; i < n; i++ )
                                if (readers[i].next(key,value)) {
                                    keys[i].set(key.data());
                                    values[i].set(value.data());
                                } else {
                                    keys[i] = null;
                                    readers[i].close();
                                }
                        } catch (IOException e) {
                            throw new Error("Cannot merge values from an intermediate result");
                        };
                    min = -1;
                    for ( int i = 0; i < n; i++ )
                        if (keys[i] != null && min < 0)
                            min = i;
                        else if (keys[i] != null && keys[i].compareTo(keys[min]) < 0)
                            min = i;
                    return min >= 0;
                }
                public MRData next () {
                    try {
                        MRData res = values[min].data();
                        if (readers[min].next(key,value)) {
                            keys[min].set(key.data());
                            values[min].set(value.data());
                        } else {
                            keys[min] = null;
                            readers[min].close();
                        };
                        return res;
                    } catch (IOException e) {
                        throw new Error("Cannot merge values from an intermediate result");
                    }
                }
            });
    }

    /** The collect physical operator */
    public final static Bag collect ( final DataSet x, boolean strip ) throws Exception {
        return Evaluator.evaluator.parsedInputFormat().newInstance().collect(x,strip);
    }

    /** The collect physical operator */
    public final static Bag collect ( final DataSet x ) throws Exception {
        return collect(x,true);
    }

    /** the DataSet union physical operator */
    public final static DataSet merge ( final DataSet x, final DataSet y ) throws IOException {
        DataSet res = x;
        res.source.addAll(y.source);
        return res;
    }

    final static MR_long counter_key = new MR_long(0);
    final static MRContainer counter_container = new MRContainer(counter_key);
    final static MRContainer value_container = new MRContainer(new MR_int(0));

    /** The cache operator that dumps a bag into an HDFS file */
    public final static DataSet fileCache ( Bag s ) throws IOException {
        String newpath = new_path(conf);
        Path path = new Path(newpath);
        FileSystem fs = path.getFileSystem(conf);
        SequenceFile.Writer writer
            = new SequenceFile.Writer(fs,conf,path,
                                      MRContainer.class,MRContainer.class);
        long i = 0;
        for ( MRData e: s ) {
            counter_key.set(i++);
            value_container.set(e);
            writer.append(counter_container,value_container);
        };
        writer.close();
        return new DataSet(new BinaryDataSource(0,newpath,conf),0,0);
    }

    /** for dumped data to a file, return the MRQL type of the data */
    public final static Tree get_type ( String file ) {
        try {
            Path path = new Path(file);
            FileSystem fs = path.getFileSystem(conf);
            BufferedReader ftp = new BufferedReader(new InputStreamReader(fs.open(path.suffix(".type"))));
            String s[] = ftp.readLine().split("@");
            ftp.close();
            if (s.length != 2 )
                return null;
            if (!s[0].equals("2"))
                throw new Error("The binary file has been created in java mode and cannot be read in hadoop mode");
            return Tree.parse(s[1]);
        } catch (Exception e) {
            return null;
        }
    }

    /** create a new PrintStream from the file */
    final static PrintStream print_stream ( String file )  throws Exception {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        return new PrintStream(fs.create(path));
    }
}
