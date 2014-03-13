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

import java_cup.runtime.*;
import java.io.*;
import java.util.HashMap;


abstract public class Tree implements Serializable {

    /* the current line number in the parsed file */
    public static int line_number = 0;

    /* the current char position in the parsed file */
    public static int position_number = 0;

    /* true when Trees are parsed rather than processed */
    public static boolean parsed = false;

    /* the line number of the Tree in the parsed file */
    public int line;

    /* the char position of the Tree in the parsed file */
    public int position;

    Tree () { line = line_number; position = position_number; }

    /* deep equality */
    public abstract boolean equals ( Tree e );

    final public boolean is_node () { return (this instanceof Node); }
    final public boolean is_variable () { return (this instanceof VariableLeaf); }
    final public boolean is_long () { return (this instanceof LongLeaf); }
    final public boolean is_string () { return (this instanceof StringLeaf); }
    final public boolean is_double () { return (this instanceof DoubleLeaf); }

    final public String variableValue () { return (this instanceof VariableLeaf) ? ((VariableLeaf)this).value() : ""; }
    final public long longValue () { return (this instanceof LongLeaf) ? ((LongLeaf)this).value() : (long)0; }
    final public String stringValue () { return (this instanceof StringLeaf) ? ((StringLeaf)this).value() : ""; }
    final public double doubleValue () { return (this instanceof DoubleLeaf) ? ((DoubleLeaf)this).value() : (double)0.0; }

    /* size used for pretty() */
    protected abstract int size ();

    /* print the Tree into a string */
    public abstract String toString ();

    /* pretty-print the Tree padded with position space characters */
    public abstract String pretty ( int position );

    private static Tree fix_tree ( Tree e ) {
        if (e instanceof Node) {
            Trees cs = Trees.nil;
            for ( Tree a: ((Node) e).children().tail() )
                cs = cs.append(fix_tree(a));
            return new Node(((VariableLeaf)(((Node) e).children().head())).value(),cs);
        } else return e;
    }

    /* the inverse of toString() */
    final public static synchronized Tree parse ( String s ) throws Exception {
        GenParser.scanner = new GenLex(new StringReader("#<"+s+">"));
        GenParser.out = new PrintStream(new ByteArrayOutputStream());
        new GenParser(GenParser.scanner).parse();
        return fix_tree(GenParser.parse_tree);
    }

    private static HashMap<String,String> names = new HashMap<String,String>(1000);

    public static String add ( String s ) {
        String ns = names.get(s);
        if (ns == null) {
            names.put(s,s);
            return s;
        } else return ns;
    }
}
