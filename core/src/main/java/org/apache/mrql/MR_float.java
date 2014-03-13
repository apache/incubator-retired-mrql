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


/** a container for float values */
final public class MR_float extends MRData {
    private float value;

    public MR_float ( float x ) { value = x; }

    public MR_float ( double x ) { value = (float)x; }

    public void materializeAll () {};

    public float get () { return value; }

    public void set ( float v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.FLOAT);
        out.writeFloat(value);
    }

    final public static MR_float read ( DataInput in ) throws IOException {
        return new MR_float(in.readFloat());
    }

    public void readFields ( DataInput in ) throws IOException {
        value = in.readFloat();
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof MR_float);
        float v = ((MR_float) x).value;
        return (value == v) ? 0 : ((value > v) ? 1 : -1);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        size[0] = (Float.SIZE >> 3)+1;
        float v = WritableComparator.readFloat(x,xs) - WritableComparator.readFloat(y,ys);
        return (v == 0) ? 0 : ((v > 0) ? 1 : -1);
    }

    public boolean equals ( Object x ) {
        return x instanceof MR_float && ((MR_float)x).value==value;
    }

    public int hashCode () {
        return Math.abs(Float.floatToIntBits(value));
    }

    public String toString () {
        return Float.toString(value);
    }
}
