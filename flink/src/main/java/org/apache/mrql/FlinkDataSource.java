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
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.*;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;


/** A data source for storing a Flink result (a Flink DataSet ) */
final public class FlinkDataSource extends DataSource implements Serializable {
    public DataSet<FData> data_set;

    /** Used in MRQL total aggregations in FlinkEvaluator.aggregate */
    public static final class Reducer implements Accumulator<FData,String> {
        public FData accumulated_value;
        public FData zero;
        public String acc_fnc;
        public String merge_fnc;
        transient Function acc;
        transient Function merge;

        public Reducer () {}

        public Reducer ( MRData zero, String acc_fnc, String merge_fnc ) {
            this.zero = new FData(zero);
            accumulated_value = this.zero;
            this.acc_fnc = acc_fnc;
            this.merge_fnc = merge_fnc;
        }

        public void add ( FData value ) {
            if (acc == null)
                try {
                    acc = Interpreter.evalF(Tree.parse(acc_fnc),null);
                } catch (Exception ex) {
                    throw new Error(ex);
                };
            accumulated_value = new FData(acc.eval(new Tuple(accumulated_value.data(),value.data())));
        }

        public String getLocalValue () {
            // due to a Flink bug, we cannot return a custom object (FData) from an accumulator
            return accumulated_value.data().toString();
        }

        public void resetLocal () {
            accumulated_value = zero;
        }

        public void merge ( Accumulator<FData,String> other ) {
            if (merge == null)
                try {
                    merge = Interpreter.evalF(Tree.parse(merge_fnc),null);
                } catch (Exception ex) {
                    throw new Error(ex);
                };
            accumulated_value = new FData(merge.eval(new Tuple(accumulated_value.data(),
                                                               ((Reducer)other).accumulated_value.data())));
        }

        @Override
        public Accumulator<FData, String> clone() {
            Reducer reducer = new Reducer();

            reducer.zero = new FData(this.zero.data);
            reducer.accumulated_value = new FData(accumulated_value.data);
            reducer.acc = this.acc;
            reducer.merge = this.merge;
            reducer.acc_fnc = this.acc_fnc;
            reducer.merge_fnc = this.merge_fnc;

            return reducer;
        }

        public void write ( DataOutputView out ) throws IOException {
            accumulated_value.write(out);
            zero.write(out);
            out.writeUTF(acc_fnc);
            out.writeUTF(merge_fnc);
        }

        public void read ( DataInputView in ) throws IOException {
            accumulated_value = new FData();
            zero = new FData();
            accumulated_value.read(in);
            zero.read(in);
            acc_fnc = in.readUTF();
            merge_fnc = in.readUTF();
        }
    }

    FlinkDataSource ( DataSet<FData> d, String path, boolean to_be_merged  ) {
        super();
        this.data_set = d;
        this.path = path;
        this.inputFormat = FlinkBinaryInputFormat.class;
        this.to_be_merged = to_be_merged;
    }

    public static final class size_mapper extends RichFlatMapFunction<FData,FData> {
        private LongCounter c = new LongCounter();

        @Override
        public void open ( org.apache.flink.configuration.Configuration parameters ) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("count",c);
        }

        @Override
        public void flatMap ( FData value, Collector<FData> out ) throws Exception {
            c.add(1L);
        }
    }

    /** not used */
    @Override
    public long size ( Configuration conf ) {
        return Long.MAX_VALUE;
    }

    /** return the first num values */
    @Override
    public List<MRData> take ( int num ) {
        try {
            // used for printing query results; currently, terribly inefficient
            List<MRData> a = new ArrayList<MRData>();
            final FData container = new FData();
            if (!to_be_merged) {
                FileInputFormat<FData> sf = (new FlinkBinaryInputFormat()).inputFormat(path);
                FileInputSplit[] splits = sf.createInputSplits(1);
                long i = 0;
                for ( FileInputSplit split: splits ) {
                    sf.open(split);
                    while (!sf.reachedEnd() && (num < 0 || i < num)) {
                        a.add(sf.nextRecord(container).data());
                        i++;
                    };
                    if (num > 0 && i >= num)
                        break;
                };
                sf.close();
                return a;
            } else { // needs merging since splits came out of sorting
                FlinkBinaryInputFormat ifbif = new FlinkBinaryInputFormat();
                FileInputFormat<FData> sf = ifbif.inputFormat(path);
                FileInputSplit[] splits = sf.createInputSplits(1);
                FileInputFormat<FData>[] s = new FlinkBinaryInputFormat.FDataInputFormat[splits.length];
                MRData[] key = new MRData[splits.length];
                MRData[] value = new MRData[splits.length];
                int min = 0;
                int i = 0;
                for ( FileInputSplit split: splits ) {
                    s[i] = ifbif.inputFormat(path);
                    s[i].open(split);
                    i++;
                };
                for ( int j = 0; j < splits.length; j++ ) {
                    if (!s[j].reachedEnd()) {
                        Tuple v = (Tuple)s[j].nextRecord(container).data();
                        key[j] = v.first();
                        value[j] = v.second();
                    }
                };
                long c = 0;
                while ( (num < 0 || c < num) && min >= 0) {
                    min = -1;
                    for ( int j = 0; j < splits.length; j++ )
                        if (key[j] != null && (min < 0 || key[j].compareTo(key[min]) < 0))
                            min = j;
                    if (min >= 0) {
                        a.add(value[min]);
                        if (!s[min].reachedEnd()) {
                            Tuple v = (Tuple)s[min].nextRecord(container).data();
                            key[min] = v.first();
                            value[min] = v.second();
                        } else key[min] = null;
                    };
                    c++;
                };
                for ( int j = 0; j < splits.length; j++ )
                    s[j].close();
                return a;
            }
        } catch (IOException ex) {
            throw new Error(ex);
        }
    }

    /** merge the splits of a flink data source that have already been sorted */
    public Bag merge () {
        try {
            FlinkBinaryInputFormat ifbif = new FlinkBinaryInputFormat();
            FileInputFormat<FData> sf = ifbif.inputFormat(path);
            FileInputSplit[] splits = sf.createInputSplits(1);
            final int n = splits.length;
            final FileInputFormat<FData>[] s = new FlinkBinaryInputFormat.FDataInputFormat[n];
            int i = 0;
            for ( FileInputSplit split: splits ) {
                s[i] = ifbif.inputFormat(path);
                s[i].open(split);
                i++;
            };
            return new Bag(new BagIterator () {
                    int min = 0;
                    boolean first = true;
                    MRData[] keys = new MRData[n];
                    MRData[] values = new MRData[n];
                    MRData key, value;
                    final FData container = new FData();
                    public boolean hasNext () {
                        try {
                            if (first) {
                                first = false;
                                for ( int i = 0; i < n; i++ ) {
                                    if (!s[i].reachedEnd()) {
                                        Tuple v = (Tuple)s[i].nextRecord(container).data();
                                        keys[i] = v.first();
                                        values[i] = v.second();
                                    }
                                }
                            };
                            min = -1;
                            for ( int i = 0; i < n; i++ )
                                if (keys[i] != null && (min < 0 || keys[i].compareTo(keys[min]) < 0))
                                    min = i;
                            if (min >= 0) {
                                key = keys[min];
                                value = values[min];
                                if (!s[min].reachedEnd()) {
                                    Tuple v = (Tuple)s[min].nextRecord(container).data();
                                    keys[min] = v.first();
                                    values[min] = v.second();
                                } else keys[min] = null;
                                return true;
                            } else {
                                for ( int i = 0; i < n; i++ )
                                    s[i].close();
                                return false;
                            }
                        } catch (IOException e) {
                            throw new Error("Cannot merge values from an intermediate result: "+e);
                        }
                    }
                    public MRData next () { return value; }
                });
        } catch (IOException e) {
            throw new Error("Cannot merge values from an intermediate result: "+e);
        }
    }

    /** Used in MRQL total aggregations in FlinkEvaluator.aggregate */
    public static final class reduce_mapper extends RichFlatMapFunction<FData,FData> {
        Reducer r;

        reduce_mapper ( MRData zero, String acc, String merge ) {
            r = new Reducer(zero,acc,merge);
        }

        @Override
        public void open ( org.apache.flink.configuration.Configuration parameters ) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("reducer",r);
        }

        @Override
        public void flatMap ( FData value, Collector<FData> out ) throws Exception {
            r.add(value);
        }
    }
}
