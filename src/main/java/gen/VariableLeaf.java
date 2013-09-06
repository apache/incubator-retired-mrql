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
package org.apache.mrql.gen;

import java.io.*;


final public class VariableLeaf extends Tree {
    public String value;

    public VariableLeaf ( String s ) {
        super();
        value = Tree.add(s);
    }

    final public String value () { return value; }

    public boolean equals ( Tree e ) {
        return (e instanceof VariableLeaf)
            && value == ((VariableLeaf) e).value;
    }

    protected int size () { return value.length(); }

    public String toString () { return value; }

    public String pretty ( int position ) { return value; }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        value = Tree.add(value);
    }
}
