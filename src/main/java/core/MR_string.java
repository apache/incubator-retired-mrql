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
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


/** a container for strings */
final public class MR_string extends MRData {
    private String value;

    public MR_string ( String s ) { value = s; }

    public void materializeAll () {};

    public String get () { return value; }

    public void set ( String v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.STRING);
        Text.writeString(out,value);
    }

    final public static MR_string read ( DataInput in ) throws IOException {
        return new MR_string(Text.readString(in));
    }

    public void readFields ( DataInput in ) throws IOException {
        value = Text.readString(in);
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof MR_string);
        return value.compareTo(((MR_string) x).value);
    }

    final static Text.Comparator comparator = new Text.Comparator();

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        try {
            size[0] = 1+WritableComparator.readVInt(x,xs)+WritableUtils.decodeVIntSize(x[xs]);
            return comparator.compare(x,xs,xl,y,ys,yl);
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    public boolean equals ( Object x ) {
        return x instanceof MR_string && value.equals(((MR_string) x).value);
    }

    public int hashCode () { return value.hashCode(); }

    public String toString () {
        return "\""+value+"\"";
    }
}
