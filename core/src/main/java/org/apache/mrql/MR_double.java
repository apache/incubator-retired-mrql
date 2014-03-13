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


/** a container for double values */
final public class MR_double extends MRData {
    private double value;

    public MR_double ( double x ) { value = x; }

    public void materializeAll () {};

    public double get () { return value; }

    public void set ( double v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.DOUBLE);
        out.writeDouble(value);
    }

    final public static MR_double read ( DataInput in ) throws IOException {
        return new MR_double(in.readDouble());
    }

    public void readFields ( DataInput in ) throws IOException {
        value = in.readDouble();
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof MR_double);
        double v = ((MR_double) x).value;
        return (value == v) ? 0 : ((value > v) ? 1 : -1);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        size[0] = (Double.SIZE >> 3)+1;
        double v = WritableComparator.readDouble(x,xs) - WritableComparator.readDouble(y,ys);
        return (v == 0) ? 0 : ((v > 0) ? 1 : -1);
    }

    public boolean equals ( Object x ) {
        return x instanceof MR_double && ((MR_double)x).value==value;
    }

    public int hashCode () {
        return Math.abs((int)Double.doubleToLongBits(value));
    }

    public String toString () {
        return Double.toString(value);
    }
}
