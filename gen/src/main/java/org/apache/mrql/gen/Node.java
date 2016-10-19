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


final public class Node extends Tree {
    public String name;
    public Trees children;

    public Node ( String name, Trees children ) {
        super();
        this.name = Tree.add(name);
        this.children = children;
    }

    public Node ( String name ) {
        super();
        this.name = name;
        children = Trees.nil;
    }

    final public String name () { return name; }

    final public Trees children () { return children; }

    public boolean equals ( Tree e ) {
        return (e instanceof Node)
            && name == ((Node) e).name
            && children.equals(((Node) e).children);
    }

    protected int size () {
        return name().length()+children().size();
    }

    public String toString () {
        if (Character.isLetter(name.charAt(0))
            || !(children().length()==2))
           return name + children().toString();
        else return "(" + children().head().toString() + name
                 + children().tail().head().toString() + ")";
    }

    public String pretty ( int position ) {
        if (Character.isLetter(name.charAt(0))
            || !(children().length()==2))
           return name + children().pretty(position+name.length());
        else return "(" + children().head().toString() + name
                 + children().tail().head().toString() + ")";
    }

    public void writeData(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        name = Tree.add(name);
    }
}
