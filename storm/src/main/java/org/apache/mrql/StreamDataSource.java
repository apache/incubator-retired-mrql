/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StreamDataSource extends DataSource implements Serializable {
    Stream stream;
    long numOfRecords;
    
    public StreamDataSource(Stream stream) {
        super();
        this.stream = stream;
    }

    @Override
    public long size(Configuration conf) {
        return super.size(conf);
    }

    @Override
    public List<MRData> take(int num) {
        final List<MRData> data = new ArrayList<MRData>();
        stream.each(stream.getOutputFields(),new BaseFunction() {
            @Override
            public void execute(TridentTuple tuple, TridentCollector collector) {
               System.out.println("output: "+tuple);
               MRData value = (MRData)tuple.get(0);
               data.add(value);
           }
       },new Fields("finaloutput"));
        return data;
    }

    @Override
    public MRData reduce(MRData zero,final Function acc) {
     return null;
 }
}
