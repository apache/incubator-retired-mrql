package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


final public class MR_long implements MRData {
    private long value;

    public MR_long ( long i ) { value = i; }

    public void materializeAll () {};

    public long get () { return value; }

    public void set ( long v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
	out.writeByte(MRContainer.LONG);
	WritableUtils.writeVLong(out,value);
    }

    final public static MR_long read ( DataInput in ) throws IOException {
	return new MR_long(WritableUtils.readVLong(in));
    }

    public void readFields ( DataInput in ) throws IOException {
	value = WritableUtils.readVLong(in);
    }

    public int compareTo ( MRData x ) {
	assert(x instanceof MR_long);
	long v = ((MR_long) x).value;
	return (value == v) ? 0 : ((value > v) ? 1 : -1);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
	try {
	    size[0] = 1+WritableUtils.decodeVIntSize(x[xs]);
	    long v = WritableComparator.readVLong(x,xs)-WritableComparator.readVLong(y,ys);
	    return (v == 0) ? 0 : ((v > 0) ? 1 : -1);
	} catch (IOException e) {
	    throw new Error(e);
	}
    }

    public boolean equals ( Object x ) {
	return x instanceof MR_long && ((MR_long)x).value==value;
    }

    public int hashCode () { return (int)Math.abs(value); }

    public String toString () {
	return Long.toString(value);
    }
}
