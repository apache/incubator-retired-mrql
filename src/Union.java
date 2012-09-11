package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


public class Union implements MRData {
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
