package hadoop.mrql;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


// wraps a DataSet (stored in HDFS)
final public class MR_dataset implements MRData {
    public DataSet dataset;

    public MR_dataset ( DataSet d ) { dataset = d; }

    public void materializeAll () {};

    public DataSet dataset () { return dataset; }

    final public void write ( DataOutput out ) throws IOException {
	try {
	    Plan.collect(dataset).write(out);
	} catch (Exception ex) {
	    throw new Error(ex);
	}
    }

    public void readFields ( DataInput in ) throws IOException {
	throw new Error("DataSets are not serializable");
    }

    public int compareTo ( MRData x ) {
	throw new Error("DataSets cannot be compared");
    }

    public boolean equals ( Object x ) {
	throw new Error("DataSets cannot be compared");
    }
}
