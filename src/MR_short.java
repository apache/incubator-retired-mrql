package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

final public class MR_short implements MRData {
    private short value;

    public MR_short ( short i ) { value = i; }

    public void materializeAll () {};

    public short get () { return value; }

    public void set ( short v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
	out.writeByte(MRContainer.SHORT);
	out.writeShort(value);
    }

    final public static MR_short read ( DataInput in ) throws IOException {
	return new MR_short(in.readShort());
    }

    public void readFields ( DataInput in ) throws IOException {
	value = in.readShort();
    }

    public int compareTo ( MRData x ) {
	assert(x instanceof MR_short);
	return value - ((MR_short) x).value;
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
	size[0] = (Short.SIZE >> 3)+1;
	return WritableComparator.readUnsignedShort(x,xs) - WritableComparator.readUnsignedShort(y,ys);
    }

    public boolean equals ( Object x ) {
	return x instanceof MR_short && ((MR_short)x).value==value;
    }

    public int hashCode () { return Math.abs((int)value); }

    public String toString () {
	return Short.toString(value);
    }
}
