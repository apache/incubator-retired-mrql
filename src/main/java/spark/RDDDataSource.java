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
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;


final public class RDDDataSource extends DataSource implements Serializable {
    public JavaRDD<MRData> rdd;

    RDDDataSource ( JavaRDD<MRData> rdd ) {
        super();
        this.rdd = rdd;
    }

    @Override
    public long size ( Configuration conf ) {
        return rdd.count();
    }

    /** return the first num values */
    @Override
    public List<MRData> take ( int num ) {
        return (num < 0) ? rdd.collect() : rdd.take(num);
    }

    /** accumulate all dataset values */
    @Override
    public MRData reduce ( final MRData zero, final Function acc ) {
        return rdd.aggregate(zero,new Function2<MRData,MRData,MRData>() {
                Tuple t = new Tuple(new Tuple(),new Tuple());
                public MRData call ( MRData x, MRData y ) {
                    t.set(0,x).set(1,y);
                    return acc.eval(t);
                }
            },new Function2<MRData,MRData,MRData>() {
                Tuple t = new Tuple(new Tuple(),new Tuple());
                public MRData call ( MRData x, MRData y ) {
                    return (y.equals(zero)) ? x : y;
                }
            });
    }
}
