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

/** a container for boolean values */
final public class MR_bool extends MRData {
    private boolean value;

    public MR_bool ( boolean i ) { value = i; }

    public void materializeAll () {};

    public boolean get () { return value; }

    public void set ( boolean v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.BOOLEAN);
        out.writeBoolean(value);
    }

    final public static MR_bool read ( DataInput in ) throws IOException {
        return new MR_bool(in.readBoolean());
    }

    public void readFields ( DataInput in ) throws IOException {
        value = in.readBoolean();
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof MR_bool);
        return (value == ((MR_bool) x).value) ? 0 : (value ? 1 : -1);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        size[0] = 2;
        boolean bx = x[xs] > 0;
        boolean by = y[ys] > 0;
        return (bx == by) ? 0 : (bx ? 1 : -1);
    }

    public boolean equals ( Object x ) {
        return x instanceof MR_bool && ((MR_bool)x).value==value;
    }

    public int hashCode () { return value ? 0 : 1; }

    public String toString () {
        return Boolean.toString(value);
    }
}
