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

import java.util.Iterator;
import java.io.*;


final class TreeIterator implements Iterator<Tree> {
    Trees trees;

    TreeIterator ( Trees trees ) { this.trees = trees; }

    public boolean hasNext () { return trees.tail != null; }

    public Tree next () {
        Tree res = trees.head;
        trees = trees.tail;
        return res;
    }

    public void remove () { trees = trees.tail; }
}


final public class Trees implements Iterable<Tree>, Serializable {
    private final static int screen_size = 100;
    public Tree   head;
    public Trees tail;

    public Trees ( Tree head, Trees tail ) {
        if (tail == null)
            throw new Error("Gen: an empty list of nodes must be nil, not null");
        this.head = head;
        this.tail = tail;
    }

    public Trees () {
        head = null;
        tail = null;
    }

    public final static Trees nil = new Trees();

    public Trees ( Tree head ) {
        this.head = head;
        tail = nil;
    }

    public Tree head () {
        if (tail == null)
           throw new Error("Gen: tried to retrieve the head of an empty list of nodes");
        return head;
    }

    public Trees tail () {
        if (tail == null)
           throw new Error("Gen: tried to retrieve the tail of an empty list of nodes");
        return tail;
    }

    public boolean is_empty () {
        return (tail == null);
    }

    /* number of nodes */
    public int length () {
        int n = 0;
        for (Trees r = this; !r.is_empty(); r = r.tail)
            n += 1;
        return n;
    }

    /* put an Tree e at the beginning of the nodes */
    public Trees cons ( Tree e ) {
        return new Trees(e,this);
    }

    /* put an Tree e at the end of the nodes */
    public Trees append ( Tree e ) {
        if (is_empty())
            return new Trees(e);
        else {
            Trees temp = new Trees(e,new Trees(e));
            Trees res = temp;
            for (Trees r = this; !r.is_empty(); r = r.tail) {
                temp.tail = temp.tail.cons(r.head);
                temp = temp.tail;
            };
            return res.tail;
        }
    }

    /* append two lists of nodes */
    public Trees append ( Trees s ) {
        if (is_empty())
           return s;
        else if (s.is_empty())
           return this;
        else {
           Trees temp = s.cons(s.head);
           Trees res = temp;
           for (Trees r = this; !r.is_empty(); r = r.tail)
           {   temp.tail = temp.tail.cons(r.head);
               temp = temp.tail;
           }
           return res.tail;
        }
    }

    /* reverse the order of nodes */
    public Trees reverse () {
        Trees res = nil;
        for (Trees r = this; !r.is_empty(); r = r.tail)
            res = res.cons(r.head);
        return res;
    }

    /* is e one of the nodes? */
    public boolean member ( Tree e ) {
        for (Trees r = this; !r.is_empty(); r = r.tail)
            if (r.head.equals(e))
                return true;
        return false;
    }

    /* return the nth node */
    public Tree nth ( int n ) {
        Trees r = this;
        for (int i = 0; !r.is_empty() && i < n; r = r.tail(), i++)
            ;
        if (r.is_empty())
            throw new Error("Gen: tried to retrieve a nonexistent nth element from a list of nodes");
        else return r.head;
    }

    /* deep equality */
    public boolean equals ( Trees s ) {
        Trees n = this;
        Trees m = s;
        for(; n.tail != null && m.tail != null; n = n.tail, m = m.tail )
            if (!n.head.equals(m.head))
                return false;
        return (m.tail == null) && (n.tail == null);
    }

    protected int size () {
        int n = 1;
        for (Trees r = this; !r.is_empty(); r = r.tail)
            n += r.head.size()+1;
         return n;
    }

    public Iterator<Tree> iterator () { return new TreeIterator(this); }

    /* print the nodes */
    public String toString () {
        if (is_empty())
            return "()";
        String s = "(" + head;
        for (Trees r = tail; !r.is_empty(); r = r.tail)
            s = s + "," + r.head;
        return s + ")";
    }

    /* pretty-print the nodes */
    public String pretty ( int position ) {
        if (is_empty() || (position+size() <= screen_size))
            return toString();
        String s = "(" + head.pretty(position+1);
        for (Trees r=tail; !r.is_empty(); r=r.tail) {
            s = s + ",\n";
            for (int i=0; i<position+1; i++)
                s = s + " ";
            s = s + r.head.pretty(position+1);
        };
        return s + ")";
    }
}
