/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql;

import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;


/** The domain of the MRQL physical algebra is a set of DataSources */
public class DataSet {
    public ArrayList<DataSource> source;  // multiple sources
    public long counter;  // a Hadoop user-defined counter used by the `repeat' operator
    public long records;  // total number of dataset records

    /** Construct a DataSet that contains one DataSource
     * @param s the given DataSource
     * @param counter a Hadoop user-defined counter used by the `repeat' operator
     * @param records total number of dataset records
     */
    DataSet ( DataSource s, long counter, long records ) {
        source = new ArrayList<DataSource>();
        source.add(s);
        this.counter = counter;
        this.records = records;
    }

    /** Construct a set of DataSources
     * @param counter a Hadoop user-defined counter used by the `repeat' operator
     * @param records total number of dataset records
     */
    DataSet ( long counter, long records ) {
        source = new ArrayList<DataSource>();
        this.counter = counter;
        this.records = records;
    }

    /** add a DataSource to this DataSet */
    public void add ( DataSource s ) {
        source.add(s);
    }

    /** merge this DataSet with the given DataSet */
    public void merge ( DataSet ds ) {
        source.addAll(ds.source);
        counter += ds.counter;
        records += ds.records;
    }

    /** dataset size in bytes */
    public long size ( Configuration conf ) {
        long n = 0;
        for (DataSource s: source)
            n += s.size(conf);
        return n;
    }

    /** return a single DataSource path by merging all the DataSource paths in this DataSet */
    public String merge () {
        Object[] ds = source.toArray();
        String path = ((DataSource)ds[0]).path.toString();
        for ( int i = 1; i < ds.length; i++ )
            path += ","+((DataSource)ds[i]).path;
        return path;
    }

    /** return the first num values */
    public List<MRData> take ( int num ) {
        int count = num;
        ArrayList<MRData> res = new ArrayList<MRData>();
        for ( DataSource s: source ) {
            res.addAll(s.take(count));
            if (res.size() < count)
                count = count-res.size();
            else return res;
        };
        return res;
    }

    /** accumulate all dataset values */
    public MRData reduce ( MRData zero, Function acc ) {
        MRData res = zero;
        for ( DataSource s: source )
            res = s.reduce(res,acc);
        return res;
    }

    public String toString () {
        String p = "<"+counter;
        for (DataSource s: source)
            p += ","+s;
        return p+">";
    }
}
