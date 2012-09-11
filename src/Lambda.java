package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


final public class Lambda implements MRData {
    private Function lambda;

    public Lambda ( Function f ) { lambda = f; }

    public void materializeAll () {};

    public Function lambda () { return lambda; }

    final public void write ( DataOutput out ) throws IOException {
	throw new Error("Functions are not serializable");
    }

    public void readFields ( DataInput in ) throws IOException {
	throw new Error("Functions are not serializable");
    }

    public int compareTo ( MRData x ) {
	throw new Error("Functions cannot be compared");
    }

    public boolean equals ( Object x ) {
	throw new Error("Functions cannot be compared");
    }
}
