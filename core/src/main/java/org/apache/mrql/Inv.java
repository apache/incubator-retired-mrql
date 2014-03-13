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


final public class Inv extends MRData {
    private MRData value;

    Inv ( MRData v ) { value = v; }

    public void materializeAll () { value.materializeAll(); };

    public MRData value () { return value; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.INV);
        value.write(out);
    }

    final public static Inv read ( DataInput in ) throws IOException {
        return new Inv(MRContainer.read(in));
    }

    public void readFields ( DataInput in ) throws IOException {
        value.readFields(in);
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof Inv);
        return -value.compareTo(((Inv)x).value);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        int n = MRContainer.compare(x,xs,xl,y,ys,yl,size);
        size[0] += 1;
        return -n;
    }

    public boolean equals ( Object x ) {
        return x instanceof Inv && value.equals(((Inv)x).value);
    }

    public int hashCode () { return value.hashCode(); }

    public String toString () { return "inv("+value.toString()+")"; }
}
