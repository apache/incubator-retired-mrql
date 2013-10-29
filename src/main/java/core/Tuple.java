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

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


/** a container for Tuples */
final public class Tuple extends MRData {
    private final static long serialVersionUID = 723385754575L;

    MRData[] tuple;

    public Tuple ( int size ) {
        tuple = new MRData[size];
    }

    public Tuple ( final MRData ...as ) {
        tuple = as;
    }

    /** the number of elements in the tuple */
    public short size () { return (short)tuple.length; }

    public void materializeAll () {
        for (MRData e: tuple)
            e.materializeAll();
    };

    /** the i'th element of the tuple */
    public MRData get ( int i ) {
        return tuple[i];
    }

    /** the first element of the tuple */
    public MRData first () { return tuple[0]; }

    /** the second element of the tuple */
    public MRData second () { return tuple[1]; }

    /** replace the i'th element of a tuple with new data and return a new value */
    public MRData set ( int i, MRData data, MRData ret ) {
        tuple[i] = data;
        return ret;
    }

    /** replace the i'th element of a tuple with new data */
    public Tuple set ( int i, MRData data ) {
        tuple[i] = data;
        return this;
    }

    final public void write ( DataOutput out ) throws IOException {
        if (tuple.length == 0)
            out.writeByte(MRContainer.NULL);
        else if (tuple.length == 2) {
            out.writeByte(MRContainer.PAIR);
            tuple[0].write(out);
            tuple[1].write(out);
        } else if (tuple.length == 3) {
            out.writeByte(MRContainer.TRIPLE);
            tuple[0].write(out);
            tuple[1].write(out);
            tuple[2].write(out);
        } else {
            out.writeByte(MRContainer.TUPLE);
            WritableUtils.writeVInt(out,tuple.length);
            for (short i = 0; i < tuple.length; i++)
                tuple[i].write(out);
        }
    }

    final public static Tuple read ( DataInput in ) throws IOException {
        int n = WritableUtils.readVInt(in);
        Tuple t = new Tuple(n);
        for ( short i = 0; i < n; i++ )
            t.tuple[i] = MRContainer.read(in);
        return t;
    }

    final public static Tuple read2 ( DataInput in ) throws IOException {
        return new Tuple(MRContainer.read(in),MRContainer.read(in));
    }

    final public static Tuple read3 ( DataInput in ) throws IOException {
        return new Tuple(MRContainer.read(in),MRContainer.read(in),MRContainer.read(in));
    }

    public void readFields ( DataInput in ) throws IOException {
        int n = WritableUtils.readVInt(in);
        tuple = new Tuple[n];
        for ( short i = 0; i < n; i++ )
            tuple[i] = MRContainer.read(in);
    }

    public int compareTo ( MRData x ) {
        assert(x instanceof Tuple);
        Tuple t = (Tuple) x;
        for ( short i = 0; i < tuple.length && i < t.tuple.length; i++ ) {
            int c = get(i).compareTo(t.get(i));
            if (c < 0)
                return -1;
            else if (c > 0)
                return 1;
        };
        if (tuple.length > t.tuple.length)
            return 1;
        else if (tuple.length < t.tuple.length)
            return -1;
        else return 0;
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        try {
            int n = WritableComparator.readVInt(x,xs);
            int s = WritableUtils.decodeVIntSize(x[xs]);
            for ( short i = 0; i < n; i++ ) {
                int k = MRContainer.compare(x,xs+s,xl-s,y,ys+s,yl-s,size);
                if (k != 0)
                    return k;
                s += size[0];
            };
            size[0] = s+1;
            return 0;
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    final public static int compare2 ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        int k = MRContainer.compare(x,xs,xl,y,ys,yl,size);
        if (k != 0)
            return k;
        int s = size[0];
        k = MRContainer.compare(x,xs+s,xl-s,y,ys+s,yl-s,size);
        if (k != 0)
            return k;
        size[0] += s+1;
        return 0;
    }

    final public static int compare3 ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        int k = MRContainer.compare(x,xs,xl,y,ys,yl,size);
        if (k != 0)
            return k;
        int s = size[0];
        k = MRContainer.compare(x,xs+s,xl-s,y,ys+s,yl-s,size);
        if (k != 0)
            return k;
        s += size[0];
        k = MRContainer.compare(x,xs+s,xl-s,y,ys+s,yl-s,size);
        if (k != 0)
            return k;
        size[0] += s+1;
        return 0;
    }

    public boolean equals ( Object x ) {
        if (!(x instanceof Tuple))
            return false;
        Tuple xt = (Tuple) x;
        if (xt.tuple.length != tuple.length)
            return false;
        for ( short i = 0; i < tuple.length; i++ )
            if (!xt.get(i).equals(get(i)))
                return false;
        return true;
    }

    public int hashCode () {
        int h = 127;
        for ( short i = 1; i < tuple.length; i++ )
            h ^= get(i).hashCode();
        return Math.abs(h);
    }

    public String toString () {
        if (size() == 0)
            return "()";
        String s = "("+get((short)0);
        for ( short i = 1; i < tuple.length; i++ )
            s += ","+get(i);
        return s+")";
    }
}
