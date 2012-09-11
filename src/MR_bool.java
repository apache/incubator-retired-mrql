package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

final public class MR_bool implements MRData {
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
