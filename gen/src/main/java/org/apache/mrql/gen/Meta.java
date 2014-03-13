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
import java.util.HashMap;
import java.util.Random;


final public class Meta extends Crypt {

    final public static Tree mark = new LongLeaf(-1);

    static int level = 0;

    private static int name_counter = 0;

    public static Tree new_name () {
            return new VariableLeaf("N" + (name_counter++) + "_");
    }

    public static Tree escape ( Tree e ) {
        if (Tree.parsed) {
            Tree.line_number = e.line;
            Tree.position_number = e.position;
        };
        return e;
    }

    public static void clear () {
        name_counter = 0;
        name_index = 0;
        name_vars.clear();
        package_name = "PatternNames_"+Math.abs(random.nextLong());
    }

    public static Tree escape ( String s ) { return new VariableLeaf(s); }

    public static Tree escape ( long n ) { return new LongLeaf(n); }

    public static Tree escape ( double n ) { return new DoubleLeaf(n); }

    private static HashMap<String,Integer> name_vars = new HashMap<String,Integer>(1000);

    private static int name_index = 0;

    private final static Random random = new Random();

    private static String package_name = "PatternNames_"+Math.abs(random.nextLong());

    private static String name ( String s ) {
        Integer ns = name_vars.get(s);
        if (ns == null) {
            ns = new Integer(name_index++);
            name_vars.put(s,ns);
        };
        return package_name+".P_"+ns.intValue();
    }

    static void dump_names ( PrintStream out ) {
        if (name_vars.isEmpty())
            return;
        out.println("abstract class "+package_name+" {");
        for ( String s: name_vars.keySet() )
            out.println(" final static String P_"+name_vars.get(s)+" = Tree.add("+s+");");
        out.println("}");
    }

    public static Trees subst_list ( Tree term, Tree value, Trees es ) {
        Trees res = Trees.nil;
        for (Trees r = es; !r.is_empty(); r = r.tail)
            res = res.cons(subst_expr(term,value,r.head));
        return res.reverse();
    }

    public static Tree subst_expr ( Tree term, Tree value, Tree e ) {
        if (e.equals(term))
            return value;
        else if (e instanceof Node)
            return new Node(((Node)e).name,
                            subst_list(term,value,((Node)e).children));
        else return e;
    }

    public static Tree substitute ( Trees path, Tree e, Tree value ) {
        if (path.is_empty())
            return value;
        else {
            Trees res = Trees.nil;
            for (Trees r = ((Node)e).children; !r.is_empty(); r = r.tail)
                if (r.head.equals(path.head))
                    res = res.cons(substitute(path.tail,r.head,value));
                else res = res.cons(r.head);
            return new Node(((Node)e).name,res.reverse());
        }
    }

    public static String reify ( Tree e ) {
        if (e instanceof LongLeaf)
            return "new LongLeaf(" + e + ")";
        else if (e instanceof DoubleLeaf)
            return "new DoubleLeaf(" + e + ")";
        else if (e instanceof VariableLeaf)
            if (((VariableLeaf)e).value.equals("_any_"))
                throw new Error("Gen: Cannot use \"_\" (any) in Tree Construction: "+e);
            else return "new VariableLeaf(" + encrypt(((VariableLeaf)e).value) + ")";
        else if (e instanceof StringLeaf)
            return "new StringLeaf(" + e + ")";
        else {
            Node n = (Node) e;
            if (n.name.equals("Node")) {
                String s = "new Node(";
                if (n.children.head instanceof VariableLeaf)
                    if (((VariableLeaf)n.children.head).value.equals("_any_"))
                        throw new Error("Gen: Cannot use \"_\" (any) in AST node name: "+e);
                    else s = s + encrypt(((VariableLeaf)n.children.head).value);
                else {
                    Node m = (Node) n.children.head;
                    if (m.name.equals("Code"))
                        s = s + ((StringLeaf)m.children.head).value;
                    else if (m.name.equals("Escape"))
                        s = s + ((VariableLeaf)m.children.head).value;
                };
                s = s + ",Trees.nil";
                for (Trees  r= n.children.tail; !r.is_empty(); r = r.tail)
                    if (r.head instanceof Node && ((Node)r.head).name.equals("Dots")) {
                        Node m = (Node) r.head;
                        if (m.children.is_empty())
                            throw new Error("Gen: Cannot use \"...\" in Tree construction: "+e);
                        else if (m.children.head instanceof VariableLeaf)
                            s = s + ".append(" + ((VariableLeaf)m.children.head).value + ")";
                        else s = s + ".append(" + ((StringLeaf)m.children.head).value + ")";
                    } else s = s + ".append(" + reify(r.head) + ")";
                return s + ")";
            } else if (n.name.equals("Code"))
                return "Meta.escape(" + ((StringLeaf)n.children.head).value + ")";
            else if (n.name.equals("Escape"))
                return "Meta.escape(" + ((VariableLeaf)n.children.head).value + ")";
            else if (n.name.equals("Higher"))
                return "Meta.substitute(" + ((VariableLeaf)n.children.head).value
                    + ".tail," + ((VariableLeaf)n.children.head).value
                    + ".head," + reify(n.children.tail.head) + ")";
            else throw new Error("Gen: Wrong Tree construction: "+e);
        }
    }

    public static Condition pattern_list ( Trees args, String v ) {
        if (args.is_empty())
            return new Condition("",v+".tail==null",0);
        else if (args.head instanceof Node && ((Node)args.head).name.equals("Dots")) {
            if (args.tail.is_empty()) {
                Node m = (Node) args.head;
                if (m.children.is_empty())
                    return new Condition("","true",0);
                else if (m.children.head instanceof VariableLeaf)
                    return new Condition("Trees " + ((VariableLeaf)m.children.head).value + " = " + v + "; ",
                                         "true",0);
                else if (m.children.head instanceof StringLeaf)
                    new Condition("",v + ".head.equals(" + ((StringLeaf)m.children.head).value + ")",0);
                return new Condition("","true",0);
            } else {
                Node m = (Node) args.head;
                if (m.children.is_empty()) {
                    Condition rest = pattern_list(args.tail,"R_");
                    return new Condition("Trees R_ = " + v + "; for(; R_.tail!=null && !(" + rest.pred
                                         + "); R_=R_.tail) ; if (R_.tail!=null) { " + rest.stmt,
                                         "true",rest.unmatched_brackets+1);
                } else if (m.children.head instanceof VariableLeaf) {
                    String nm = ((VariableLeaf)m.children.head).value;
                    Condition rest = pattern_list(args.tail,nm+"_");
                    return new Condition("Trees " + nm + " = Trees.nil; Trees " + nm + "_=" + v + "; "
                                         + "for(; " + nm + "_.tail!=null && !(" + rest.pred + "); "
                                         + nm + "_=(FOUND_" + Meta.level + ")?" + nm + "_:" + nm + "_.tail) " + nm + " = " + nm + ".append("
                                         + nm + "_.head); " + "if (" + nm + "_.tail!=null) { " + rest.stmt,
                                         "true",rest.unmatched_brackets+1);
                } else if (m.children.head instanceof StringLeaf) {
                    Condition rest = pattern_list(args.tail,v);
                    return new Condition(rest.stmt,
                                         v + ".equals(" + ((StringLeaf)m.children.head).value + ")" + rest.and(),
                                         rest.unmatched_brackets);
                };
                return new Condition("","true",0);
            }
        } else {
            Condition c = pattern(args.head,v+".head");
            Condition rest = pattern_list(args.tail,v+".tail");
            return new Condition(c.stmt + rest.stmt,
                                 v + ".tail!=null" + c.and() + rest.and(),
                                 c.unmatched_brackets+rest.unmatched_brackets);
        }
    }

    public static Condition pattern ( Tree e, String v ) {
        if (e instanceof LongLeaf)
            return new Condition("", "(" + v + " instanceof LongLeaf) && ((LongLeaf)" + v + ").value==" + e,0);
        else if (e instanceof DoubleLeaf)
            return new Condition("","(" + v + " instanceof DoubleLeaf) && ((DoubleLeaf)" + v + ").value==" + e,0);
        else if (e instanceof VariableLeaf)
            if (((VariableLeaf)e).value.equals("_any_"))
                return new Condition("","true",0);
            else return new Condition("","(" + v + " instanceof VariableLeaf) && ((VariableLeaf)" + v
                                      + ").value==" + name(encrypt(((VariableLeaf)e).value)),0);
        else if (e instanceof StringLeaf)
            return new Condition("","(" + v + " instanceof StringLeaf) && ((StringLeaf)" + v + ").value.equals(" + e + ")",0);
        else {
            Node n = (Node) e;
            if (n.name.equals("Node")) {
                String p = "(" + v + " instanceof Node)";
                String s = "";
                if (n.children.head instanceof VariableLeaf) {
                    if (((VariableLeaf)n.children.head).value!="_any_")
                        p = p + " && ((Node)" + v + ").name=="
                            + name(encrypt(((VariableLeaf)n.children.head).value));
                } else if (n.children.head instanceof Node)
                    if (((Node)n.children.head).name.equals("Escape"))
                        s = "String " + ((VariableLeaf)((Node)n.children.head).children.head).value
                            + " = ((Node)" + v + ").name; ";
                    else throw new Error("Gen: Wrong Tree pattern: "+e);
                Condition c = pattern_list(n.children.tail,"((Node)" + v + ").children");
                return new Condition(s+c.stmt,p+c.and(),c.unmatched_brackets);
            } else if (n.name.equals("Escape"))
                return new Condition("Tree " + ((VariableLeaf)n.children.head).value + " = " + v + "; ",
                                     "true",0);
            else if (n.name.equals("IS")) {
                String nm = ((VariableLeaf)n.children.head).value;
                Condition c = pattern(n.children.tail.head,nm);
                return new Condition(c.stmt + "Tree " + nm + " = " + v + "; ",c.pred,0);
            } else if (n.name.equals("Code"))
                return new Condition("",v + ".equals(" + ((StringLeaf)n.children.head).value + ")",0);
            else if (n.name.equals("Higher")) {
                String nm = ((VariableLeaf)n.children.head).value;
                Condition c = pattern(n.children.tail.head,nm+"__");
                String s = reify(n.children.tail.head);
                return new Condition("Tree " + nm + "_ = " + v + "; Trees " + nm + " = Trees.nil; Trees STACK_" + nm
                                     + "_ = new Trees(" + nm + "_); while (!FOUND_" + Meta.level + " && STACK_" + nm + "_.tail!=null) { Tree "
                                     + nm + "__ = STACK_" + nm + "_.head; STACK_" + nm + "_ = STACK_" + nm + "_.tail; " + nm
                                     + " = (" + nm + "__==Meta.mark) ? " + nm + ".tail : ((" + nm + "__ instanceof Node) ? "
                                     + nm + ".cons(" + nm + "__) : " + nm + "); if (" + nm + "__ instanceof Node) STACK_" + nm
                                     + "_ = ((Node)" + nm + "__).children.append(STACK_" + nm + "_.cons(Meta.mark)); if ("
                                     + nm + "__!=Meta.mark" + c.and() + ") { if (!(" + nm + "__ instanceof Node)) " + nm + "="
                                     + nm + ".cons(" + nm + "__); " + nm + " = " + nm + ".reverse(); " + c.stmt,"true",2);
            } else throw new Error("Gen: Wrong Tree pattern: "+e);
        }
    }
}
