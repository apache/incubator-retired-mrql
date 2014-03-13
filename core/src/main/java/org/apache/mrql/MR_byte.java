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


/** a container for byte values */
final public class MR_byte extends MRData {
    private byte value;

    public MR_byte ( byte i ) { value = i; }
    public MR_byte ( int i ) { value = (byte)i; }

    public void materializeAll () {};

    public byte get () { return value; }

    public void set ( byte v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.BYTE);
        out.writeByte(value);
    }

    final public static MR_byte read ( DataInput in ) throws IOException {
        return new MR_byte(in.readByte());
    }

    public void readFields ( DataInput in ) throws IOException {
        value = in.readByte();
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof MR_byte);
        byte v = ((MR_byte) x).value;
        return (value == v) ? 0 : ((value < v) ? -1 : 1);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        size[0] = 2;
        return x[xs]-y[ys];
    }

    public boolean equals ( Object x ) {
        return x instanceof MR_byte && ((MR_byte)x).value==value;
    }

    public int hashCode () { return Math.abs(value); }

    public String toString () {
        return Integer.toString(value);
    }
}
