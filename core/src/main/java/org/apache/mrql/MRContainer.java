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
import org.apache.hadoop.io.WritableComparable;


/** A container for MRData that implements read (the deserializer) */
final public class MRContainer implements WritableComparable<MRContainer>, Serializable {
    transient MRData data;

    public final static byte BOOLEAN = 0, BYTE = 1, SHORT = 2, INT = 3, LONG = 4,
        FLOAT = 5, DOUBLE = 6, CHAR = 7, STRING = 8, PAIR = 9, TUPLE = 10, BAG = 11,
        LAZY_BAG = 12, END_OF_LAZY_BAG = 13, UNION = 14, INV = 15, LAMBDA = 16,
        VARIABLE = 17, TRIPLE = 18, NULL = 19, DATASET = 20, SYNC = 99, MORE_BSP_STEPS = 98;

    public final static byte[] type_codes
        = { BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, CHAR, STRING, NULL, PAIR, TRIPLE,
            TUPLE, BAG, LAZY_BAG, END_OF_LAZY_BAG, UNION, INV, LAMBDA, VARIABLE, SYNC };

    public final static String[] type_names
        = { "boolean", "byte", "short", "int", "long", "float", "double", "char", "string",
            "null", "pair", "triple", "tuple", "bag", "lazy_bag", "end_of_lazy_bag", "union",
            "inv", "lambda", "variable", "sync", "more_bsp_steps" };

    public static byte type_code ( String type_name ) {
        for ( byte i = 0; i < type_names.length; i ++ )
            if (type_names[i].equals(type_name))
                return type_codes[i];
        return -1;
    }

    public static boolean basic_type ( byte type_code ) {
        return type_code >= 0 && type_code <= 8;
    }

    MRContainer ( MRData d ) { data = d; }

    MRContainer () { data = null; }

    public final static MRData end_of_lazy_bag = new MR_EOLB();

    MRData data () { return data; }

    public void set ( MRData v ) { data = v; }
    final public void write ( DataOutput out ) throws IOException { data.write(out); }
    public void readFields ( DataInput in ) throws IOException { data = read(in); }
    public int compareTo ( MRContainer x ) { return data.compareTo(x.data); }
    public boolean equals ( Object x ) { return data.equals(x); }
    public int hashCode () { return data.hashCode(); }
    public String toString () { return data.toString(); }

    final public static MRData read ( DataInput in ) throws IOException {
        final byte tag = in.readByte();
        switch (tag) {
        case TUPLE: return Tuple.read(in);
        case NULL: return new Tuple(0);
        case PAIR: return Tuple.read2(in);
        case TRIPLE: return Tuple.read3(in);
        case BAG: return Bag.read(in);
        case LAZY_BAG: return Bag.lazy_read(in);
        case END_OF_LAZY_BAG: return end_of_lazy_bag;
        case UNION: return Union.read(in);
        case INV: return Inv.read(in);
        case BOOLEAN: return MR_bool.read(in);
        case BYTE: return MR_byte.read(in);
        case SHORT: return MR_short.read(in);
        case INT: return MR_int.read(in);
        case LONG: return MR_long.read(in);
        case FLOAT: return MR_float.read(in);
        case DOUBLE: return MR_double.read(in);
        case CHAR: return MR_char.read(in);
        case STRING: return MR_string.read(in);
        case SYNC: return new MR_sync();
        case MORE_BSP_STEPS: return new MR_more_bsp_steps();
        };
        throw new Error("Unrecognized MRQL type tag: "+tag);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        if (x[xs] != y[ys])
            return x[xs] - y[ys];
        switch (x[xs]) {
        case TUPLE: return Tuple.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case NULL: return 0;
        case PAIR: return Tuple.compare2(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case TRIPLE: return Tuple.compare3(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case BAG: return Bag.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case UNION: return Union.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case INV: return Inv.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case BOOLEAN: return MR_bool.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case BYTE: return MR_byte.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case SHORT: return MR_short.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case INT: return MR_int.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case LONG: return MR_long.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case FLOAT: return MR_float.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case DOUBLE: return MR_double.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case CHAR: return MR_char.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case STRING: return MR_string.compare(x,xs+1,xl-1,y,ys+1,yl-1,size);
        case SYNC: return 0;
        case MORE_BSP_STEPS: return 0;
        };
        throw new Error("Unrecognized MRQL type tag: "+x[xs]);
    }

    private void writeObject ( ObjectOutputStream out ) throws IOException {
        data.write(out);
    }

    private void readObject ( ObjectInputStream in ) throws IOException, ClassNotFoundException {
        data = read(in);
    }

    private void readObjectNoData () throws ObjectStreamException {}

    final static class MR_EOLB extends MRData {
        MR_EOLB () {}

        public void materializeAll () {};

        final public void write ( DataOutput out ) throws IOException {
            out.writeByte(MRContainer.END_OF_LAZY_BAG);
        }

        public void readFields ( DataInput in ) throws IOException {}

        public int compareTo ( MRData x ) { return 0; }

        public boolean equals ( Object x ) { return x instanceof MR_EOLB; }

        public int hashCode () { return 0; }

        public String toString () {
            return "end_of_lazy_bag";
        }
    }
}
