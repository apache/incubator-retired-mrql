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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


/** used for BSP synchronization when a peer needs to do more steps */
final public class MR_more_bsp_steps extends MRData {
    MR_more_bsp_steps () {}

    public void materializeAll () {};

    final public void write ( DataOutput out ) throws IOException {
        out.writeByte(MRContainer.MORE_BSP_STEPS);
    }

    public void readFields ( DataInput in ) throws IOException {}

    public int compareTo ( MRData x ) { return 0; }

    public boolean equals ( Object x ) { return x instanceof MR_more_bsp_steps; }

    public int hashCode () { return 0; }

    public String toString () { return "more_bsp_steps"; }
}
