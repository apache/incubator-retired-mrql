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


/** A Spark InputDStream for a stream socket */
public class SparkSocketStream extends JavaInputDStream<MRData> {
    private final static scala.reflect.ClassTag<MRData> classtag = scala.reflect.ClassTag$.MODULE$.apply(MRData.class);

    public static final class MRDataSocketStream extends InputDStream<MRData> {
        private final String host;
        private final int port;
        private Parser parser;
        private final JavaStreamingContext stream_context;

        MRDataSocketStream ( JavaStreamingContext stream_context, String host, int port, String parser_name, Trees args ) {
            super(stream_context.ssc(),classtag);
            this.host = host;
            this.port = port;
            this.stream_context = stream_context;
            Class<? extends Parser> parser_class = DataSource.parserDirectory.get(parser_name);
            if (parser_class == null)
                throw new Error("Unknown parser: "+parser_name);
            try {
                parser = parser_class.newInstance();
                parser.initialize(args);
                parser.open(host,port);
	    } catch (Exception ex) {
		throw new Error("Cannot create the parser");
	    }
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

        @Override
        public Option<RDD<MRData>> compute ( Time validTime ) {
            long duration = validTime.milliseconds();
            duration = Config.stream_window;
            long ct = System.currentTimeMillis();
            ArrayList<MRData> result = new ArrayList<MRData>();
            while ( System.currentTimeMillis() - ct < duration ) {
                String data = parser.slice();
                for ( MRData x: parser.parse(data) )
                    result.add(x);
            };
            return new Some<RDD<MRData>>(SparkEvaluator.spark_context.parallelize(result).rdd());
        }
    }

    SparkSocketStream ( JavaStreamingContext stream_context, String host, int port, String parser, Trees args ) {
        super(new MRDataSocketStream(stream_context,host,port,parser,args),classtag);
    }
}
