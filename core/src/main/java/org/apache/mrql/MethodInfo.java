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

import java.lang.reflect.Method;
import org.apache.mrql.gen.*;


/** class for storing Java method information */
final public class MethodInfo implements Comparable<MethodInfo> {
    public String name;
    public Trees signature;
    public Method method;

    MethodInfo ( String n, Trees s, Method m ) {
        name = n;
        signature = s;
        method = m;
    }

    public int compareTo ( MethodInfo x )  {
        int c = name.compareTo(x.name);
        if (c != 0)
            return c;
        if (signature.length() < x.signature.length())
            return -1;
        if (signature.length() > x.signature.length())
            return 1;
        // handles overloading: more specific method signatures first
        for ( int i = 1; i < signature.length(); i++ ) {
            int ct = TypeInference.compare_types(signature.nth(i),x.signature.nth(i));
            if (ct != 0)
                return ct;
        };
        return TypeInference.compare_types(signature.nth(0),x.signature.nth(0));
    }

    public boolean equals ( Object x ) {
        return name.equals(((MethodInfo)x).name)
               && signature.equals(((MethodInfo)x).signature);
    }
}
