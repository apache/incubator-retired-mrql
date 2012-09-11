package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


final public class MR_float implements MRData {
    private float value;

    public MR_float ( float x ) { value = x; }

    public MR_float ( double x ) { value = (float)x; }

    public void materializeAll () {};

    public float get () { return value; }

    public void set ( float v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
	out.writeByte(MRContainer.FLOAT);
	out.writeFloat(value);
    }

    final public static MR_float read ( DataInput in ) throws IOException {
	return new MR_float(in.readFloat());
    }

    public void readFields ( DataInput in ) throws IOException {
	value = in.readFloat();
    }

    public int compareTo ( MRData x ) {
	assert(x instanceof MR_float);
	float v = ((MR_float) x).value;
	return (value == v) ? 0 : ((value > v) ? 1 : -1);
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
	size[0] = (Float.SIZE >> 3)+1;
	float v = WritableComparator.readFloat(x,xs) - WritableComparator.readFloat(y,ys);
	return (v == 0) ? 0 : ((v > 0) ? 1 : -1);
    }

    public boolean equals ( Object x ) {
	return x instanceof MR_float && ((MR_float)x).value==value;
    }

    public int hashCode () {
	return Math.abs(Float.floatToIntBits(value));
    }

    public String toString () {
	return Float.toString(value);
    }
}
