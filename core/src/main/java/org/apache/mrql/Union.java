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


/** union values are tagged values, where tag is the descriminator */
public class Union extends MRData {
    private byte tag;
    private MRData value;

    public Union ( byte tag, MRData value ) {
        this.tag = tag;
        this.value = value;
    }

    public void materializeAll () { value.materializeAll(); };

    public byte tag () { return tag; }

    public MRData value () { return value; }

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.UNION);
        out.writeByte(tag);
        value.write(out);
    }

    final public static Union read ( DataInput in ) throws IOException {
        return new Union(in.readByte(),MRContainer.read(in));
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readByte();
        value = MRContainer.read(in);
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof Union);
        Union p = (Union) x;
        return (tag == p.tag)
            ? value.compareTo(p.value)
            : (tag - p.tag);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        int k = (x[xs] == y[ys]) ? MRContainer.compare(x,xs+1,xl-1,y,ys+1,yl-1,size) : (x[xs]-y[ys]);
        size[0] += 2;
        return k;
    }

    public boolean equals ( Object x ) {
        return x instanceof Union && ((Union)x).tag==tag
            && ((Union)x).value.equals(value);
    }

    public int hashCode () {
        return Math.abs(tag ^ value.hashCode());
    }

    public String toString () {
        return "union("+tag+","+value+")";
    }
}
