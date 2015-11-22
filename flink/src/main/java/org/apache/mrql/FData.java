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

import java.io.IOException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;


final public class FData implements CopyableValue<FData>, Comparable<FData> {
    MRData data;

    public FData () {}

    public FData ( MRData d ) { data = d; }

    public MRData data () { return data; }

    @Override
    public void read ( DataInputView in ) throws IOException {
        data = MRContainer.read(in);
    }

    @Override
    public void write ( DataOutputView out ) throws IOException {
        data.write(out);
    }

    @Override
    public int getBinaryLength () { return -1; }

    @Override
    public void copyTo ( FData target ) {
        target.data = data;
    }

    @Override
    public FData copy () {
        return new FData(data);
    }

    @Override
    public void copy ( DataInputView source, DataOutputView target ) throws IOException {
        data = MRContainer.read(source);
        data.write(target);
    }

    @Override
    public int compareTo ( FData x ) { return data.compareTo(x.data); }

    @Override
    public boolean equals ( Object x ) {
        return x instanceof FData && data.equals(((FData)x).data);
    }

    @Override
    public int hashCode () { return data.hashCode(); }

    @Override
    public String toString () { return data.toString(); }
}
