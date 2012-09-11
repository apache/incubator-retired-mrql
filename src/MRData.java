package hadoop.mrql;


import org.apache.hadoop.io.*;


// All MRQL data are encoded as MRData
public interface MRData extends WritableComparable<MRData> {
    public void materializeAll ();
}
