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
import java.util.Iterator;


/* A symbol in the symbol table */
class SymbolCell {
    String     name;
    Tree       binding; 
    SymbolCell next;
    SymbolCell ( String n, Tree v, SymbolCell r ) { name=n; binding=v; next=r; }
}


final class SymbolTableIterator implements Iterator<String> {
    SymbolTable table;
    SymbolTable original_table;

    SymbolTableIterator ( SymbolTable t ) {
        table = new SymbolTable();
        original_table = t;
        for ( int i = 0; i < t.symbol_table_size; i++ )
            table.symbol_table[i] = t.symbol_table[i];
        for ( int i = 0; i < t.scope_stack.length; i++ )
            table.scope_stack[i] = t.scope_stack[i];
        table.scope_stack_top = t.scope_stack_top;
    }

    public boolean hasNext () {
        if (table.scope_stack_top <= 0)
            return false;
        int n = table.scope_stack[table.scope_stack_top-1];
        if (n < 0 || !table.symbol_table[n].binding.equals(original_table.lookup(table.symbol_table[n].name))) {
            table.scope_stack_top--;
            return hasNext();
        };
        return true;
    }

    public String next () {
        int n = table.scope_stack[--table.scope_stack_top];
        String var = table.symbol_table[n].name;
        table.symbol_table[n] = table.symbol_table[n].next;
        return var;
    }

    public void remove () {}
}


public class SymbolTable implements Iterable<String> {
    final static int symbol_table_size = 997;
    final static int initial_scope_stack_length = 1000;

    SymbolCell[] symbol_table;
    int[] scope_stack;
    int scope_stack_top = 0;

    public SymbolTable () {
        symbol_table = new SymbolCell[symbol_table_size];
        scope_stack = new int[initial_scope_stack_length];
        scope_stack_top = 0;
        for (int i = 0; i < symbol_table_size; i++)
            symbol_table[i] = null;
    }

    public Iterator<String> iterator () {
        return new SymbolTableIterator(this);
    }

    /* a hashing function for strings */
    int hash ( String s ) {
        return Math.abs(s.hashCode()) % symbol_table_size;
    }

    /* insert a new item in the symbol table */
    public void insert ( String key, Tree binding ) {
        int loc = hash(key);
        symbol_table[loc] = new SymbolCell(key,binding,symbol_table[loc]);
        if (scope_stack_top >= scope_stack.length) {
            int[] v = new int[scope_stack.length*2];
            for ( int i = 0; i < scope_stack.length; i++ )
                v[i] = scope_stack[i];
            scope_stack = v;
        };
        scope_stack[scope_stack_top++] = loc;
    }

    /* replace an item with a given name in the symbol table */
    public void replace ( String key, Tree binding ) {
        int loc = hash(key);
        for (SymbolCell s = symbol_table[loc]; s != null; s=s.next)
            if (s.name.equals(key))
                s.binding = binding;
    }

    /* remove an item with a given name from the symbol table */
    public boolean remove ( String key ) {
        int loc = hash(key);
        SymbolCell prev = symbol_table[loc];
        if (prev == null)
            return false;
        if (prev.name.equals(key)) {
            symbol_table[loc] = prev.next;
            return true;
        };
        for (SymbolCell s = prev.next; s != null; s=s.next, prev=prev.next)
            if (s.name.equals(key)) {
                prev.next = s.next;
                return true;
            };
        return false;
    }

    /* lookup for an item in the symbol table */
    public Tree lookup ( String key ) {
        int loc = hash(key);
        for (SymbolCell s = symbol_table[loc]; s != null; s=s.next)
            if (s.name.equals(key))
                return s.binding;
        return null;       // if not found
    }

    /* return true if key is local */
    public boolean is_local ( String key ) {
        int loc = hash(key);
        int i = 0;
        for ( SymbolCell s = symbol_table[loc]; s != null; s = s.next, i++ )
            if (s.name.equals(key)) {
                int k = 0;
                for ( int j = scope_stack_top-1; j >= 0 && scope_stack[j] >= 0; j--)
                    if (scope_stack[j] == loc)
                        if (k++ == i)
                            return true;
                return false;
            };
        return false;       // if not found
    }

    /* start a new environment */
    public void begin_scope () {
        if (scope_stack_top >= scope_stack.length) {
            int[] v = new int[scope_stack.length*2];
            for ( int i = 0; i < scope_stack.length; i++ )
                v[i] = scope_stack[i];
            scope_stack = v;
        };
        scope_stack[scope_stack_top++] = -1;
    }

    /* pop the last environment */
    public void end_scope () {
        int i = scope_stack_top-1;
        for (; scope_stack[i]>=0 && i>0; i--) {
            int loc = scope_stack[i];
            symbol_table[loc] = symbol_table[loc].next;
        };
        scope_stack_top = i;
    }

    /* display the content of the symbol table */
    public void display () {
        SymbolCell[] s = new SymbolCell[symbol_table_size];
        for (int i = 0; i<symbol_table_size; i++)
            s[i] = symbol_table[i];
        for (int i = scope_stack_top-1; i>=0; i--)
            if (scope_stack[i] == -1)
                System.out.println("----------------");
            else {
                SymbolCell c = s[scope_stack[i]];
                s[scope_stack[i]] = c.next;
                System.out.println(c.name + ": " + c.binding);
            }
    }
}
