package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


final public class MR_char implements MRData {
    private char value;

    public MR_char ( char i ) { value = i; }

    public void materializeAll () {};

    public char get () { return value; }

    public void set ( char v ) { value = v; }

    final public void write ( DataOutput out ) throws IOException {
	out.writeByte(MRContainer.CHAR);
	out.writeChar(value);
    }

    final public static MR_char read ( DataInput in ) throws IOException {
	return new MR_char(in.readChar());
    }

    public void readFields ( DataInput in ) throws IOException {
	value = in.readChar();
    }

    public int compareTo ( MRData x ) {
	assert(x instanceof MR_char);
	return value-((MR_char) x).value;
    }

    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
	size[0] = 2;
	return x[xs]-y[ys];
    }

    public boolean equals ( Object x ) {
	return x instanceof MR_char && ((MR_char)x).value==value;
    }

    public int hashCode () { return value; }

    public String toString () {
	return Character.toString(value);
    }
}
