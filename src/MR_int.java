package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


final public class MR_int implements MRData {
    private int value;

    public MR_int ( int i ) { value = i; }

    public void materializeAll () {};

    public int get () { return value; }

    public void set ( int v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
	out.writeByte(MRContainer.INT);
	WritableUtils.writeVInt(out,value);
    }

    final public static MR_int read ( DataInput in ) throws IOException {
	return new MR_int(WritableUtils.readVInt(in));
    }

    public void readFields ( DataInput in ) throws IOException {
	value = WritableUtils.readVInt(in);
    }

    public int compareTo ( MRData x ) {
	assert(x instanceof MR_int);
	return value - ((MR_int) x).value;
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
	try {
	    size[0] = 1+WritableUtils.decodeVIntSize(x[xs]);
	    int v = WritableComparator.readVInt(x,xs)-WritableComparator.readVInt(y,ys);
	    return (v == 0) ? 0 : ((v > 0) ? 1 : -1);
	} catch (IOException e) {
	    throw new Error(e);
	}
    }

    public boolean equals ( Object x ) {
	return x instanceof MR_int && ((MR_int)x).value==value;
    }

    public int hashCode () { return Math.abs(value); }

    public String toString () {
	return Integer.toString(value);
    }
}
