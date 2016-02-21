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

import java.util.HashMap;
import java.util.ArrayList;
import scala.Option;
import scala.Some;
import scala.None;
import scala.collection.immutable.List;
import scala.collection.immutable.Nil$;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.*;


/** A Spark InputDStream for MRQL InputFormats */
public class SparkFileInputStream extends JavaInputDStream<MRData> {
    private final static scala.reflect.ClassTag<MRData> classtag = scala.reflect.ClassTag$.MODULE$.apply(MRData.class);

    public static final class MRDataInputStream extends InputDStream<MRData> {
        private final String directory;
        private final boolean is_binary;
        private final JavaStreamingContext stream_context;
        private final HashMap<String,Long> file_modification_times;

        MRDataInputStream ( JavaStreamingContext stream_context, String directory, boolean is_binary ) {
            super(stream_context.ssc(),classtag);
            this.directory = directory;
            this.is_binary = is_binary;
            this.stream_context = stream_context;
            file_modification_times = new HashMap<String,Long>();
        }

        @Override
        public void start () {}

        @Override
        public void stop () {}

        @Override
        public Duration slideDuration () {
            return new Duration(Config.stream_window);
        }

        @Override
        public List dependencies () {
            return Nil$.MODULE$;
        }

        /** return the files within the directory that have been created within the last time window */
        private ArrayList<String> new_files () {
            try {
                long ct = System.currentTimeMillis();
                Path dpath = new Path(directory);
                final FileSystem fs = dpath.getFileSystem(Plan.conf);
                final FileStatus[] ds
                    = fs.listStatus(dpath,
                                    new PathFilter () {
                                        public boolean accept ( Path path ) {
                                            return !path.getName().startsWith("_")
                                                && !path.getName().endsWith(".type");
                                        }
                                    });
                ArrayList<String> s = new ArrayList<String>();
                for ( FileStatus d: ds ) {
                    String name = d.getPath().toString();
                    if (file_modification_times.get(name) == null
                           || d.getModificationTime() >  file_modification_times.get(name)) {
                        file_modification_times.put(name,new Long(ct));
                        s.add(name);
                    }
                };
                return s;
            } catch (Exception ex) {
                throw new Error("Cannot open a new file from the directory "+directory+": "+ex);
            }
        }

        private JavaRDD<MRData> hadoopFile ( String file ) {
            return SparkEvaluator.containerData(
                          (is_binary)
                          ? SparkEvaluator.spark_context.sequenceFile(file,
                                                                      MRContainer.class,MRContainer.class,
                                                                      Config.nodes)
                          : SparkEvaluator.spark_context.hadoopFile(file,SparkParsedInputFormat.class,
                                                                    MRContainer.class,MRContainer.class,
                                                                    Config.nodes));
        }

        @Override
        public Option<RDD<MRData>> compute ( Time validTime ) {
            JavaRDD<MRData> rdd = null;
            for ( String file: new_files() )
                if (rdd == null)
                    rdd = hadoopFile(file);
                else rdd = rdd.union(hadoopFile(file));
            if (rdd == null)
                rdd = SparkEvaluator.spark_context.emptyRDD();
            return new Some<RDD<MRData>>(rdd.rdd());
        }
    }

    SparkFileInputStream ( JavaStreamingContext stream_context, String directory, boolean is_binary ) {
        super(new MRDataInputStream(stream_context,directory,is_binary),classtag);
    }
}
