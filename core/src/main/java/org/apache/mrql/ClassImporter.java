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
import java.util.*;

/** imports external Java methods into MRQL */
final public class ClassImporter {
    final static boolean trace_imported_methods = false;

    final static String[] object_methods
        = { "hashCode", "getClass", "wait", "equals", "toString", "notify", "notifyAll" };

    static Vector<MethodInfo> methods = new Vector<MethodInfo>();

    public static void load_classes () {
        if (methods == null)
            methods = new Vector<MethodInfo>();
        if (methods.size() == 0) {
            importClass("org.apache.mrql.SystemFunctions");
            //****** import your classes with user-defined functions here
        }
    }

    private static boolean object_method ( String s ) {
        for (int i = 0; i < object_methods.length; i++)
            if (object_methods[i].equals(s))
                return true;
        return false;
    }

    private static Tree getType ( Class<?> c ) {
        String cn = c.getCanonicalName();
        Class<?>[] inf = c.getInterfaces();
        if (cn.equals("org.apache.mrql.MRData"))
            return new VariableLeaf("any");
        if (cn.startsWith("org.apache.mrql.MR_"))
            return new VariableLeaf(cn.substring(19));
        if (cn.equals("org.apache.mrql.Bag"))
            return new Node("bag",new Trees(new VariableLeaf("any")));
        if (cn.equals("org.apache.mrql.Inv"))
            return new VariableLeaf("any");
        if (cn.equals("org.apache.mrql.Union"))
            return new VariableLeaf("union");
        if (cn.equals("org.apache.mrql.Lambda"))
            return new VariableLeaf("any");
        if (inf.length > 0 && inf[0].equals("org.apache.mrql.MRData"))
            return new VariableLeaf("any");
        throw new Error("Unsupported type in imported method: "+cn);
    }

    private static Trees signature ( Method m ) {
        Class<?> co = m.getReturnType();
        Class<?>[] cs = m.getParameterTypes();
        Trees as = new Trees(getType(co));
        for (int i = 0; i < cs.length; i++)
            as = as.append(getType(cs[i]));
        return as;
    }

    public static String method_name ( int method_number ) {
        return methods.get(method_number).name;
    }

    public static Trees signature ( int method_number ) {
        return methods.get(method_number).signature;
    }

    /** import all Java methods from a given Java class */
    public static void importClass ( String class_name ) {
        try {
            Method[] ms = Class.forName(class_name).getMethods();
            Vector<MethodInfo> mv = new Vector<MethodInfo>();
            for (int i = 0; i < ms.length; i++)
                if (!object_method(ms[i].getName()) && ms[i].getModifiers() == 9)
                    try {
                        Trees sig = signature(ms[i]);
                        MethodInfo m = new MethodInfo(ms[i].getName(),sig,ms[i]);
                        mv.add(m);
                        methods.add(m);
                    } catch ( Exception e ) {
                        System.out.println("Warning: method "+ms[i].getName()+" cannot be imported");
                        System.out.println(e);
                        throw new Error("");
                    };
            Collections.sort(methods);
            if (Translator.functions == null)
                Translator.functions = Trees.nil;
            for ( MethodInfo m: methods )
                Translator.functions = Translator.functions.append(new Node(m.name,m.signature));
            if (trace_imported_methods) {
                System.out.print("Importing methods: ");
                for (int i = 0; i < mv.size(); i++ )
                    System.out.print(mv.get(i).name+mv.get(i).signature.tail()
                                     +":"+mv.get(i).signature.head()+"  ");
                System.out.println();
            }
        } catch (ClassNotFoundException x) {
            throw new Error("Undefined class: "+class_name);
        }
    }

    /** import a Java method with a given name from a given Java class */
    public static void importMethod ( String class_name, String method_name ) {
        try {
            Method[] ms = Class.forName(class_name).getMethods();
            MethodInfo m = null;
            for (int i = 0; i < ms.length; i++)
                if (ms[i].getName().equals(method_name)
                    && !object_method(ms[i].getName()) && ms[i].getModifiers() == 9) {
                    Trees sig = signature(ms[i]);
                    m = new MethodInfo(ms[i].getName(),sig,ms[i]);
                    Translator.functions = Translator.functions.append(new Node(ms[i].getName(),sig));
                    break;
                };
            if (m == null)
                throw new Error("No such method: "+method_name);
            methods.add(m);
            Collections.sort(methods);
            if (trace_imported_methods)
                System.out.println("Importing method: "+m.name+m.signature.tail()
                                   +":"+m.signature.head()+"  ");
        } catch (ClassNotFoundException x) {
            throw new Error("Undefined class: "+class_name);
        }
    }

    public static void print_methods () {
        for (int i = 0; i < methods.size(); i++ ) {
            MethodInfo m = methods.get(i);
            System.out.print(" "+m.name+":"+m.signature.tail()+"->"+m.signature.head());
        };
    }

    /** return the method specification of a system method with a given name over some expressions;
     * When the method is overloaded, find the most specific (in terms of arg subtyping)
     * @param method_name the given method name
     * @param args the method expressions
     * @return the method specification
     */
    public static Tree find_method ( String method_name, Trees args ) {
        for (int i = 0; i < methods.size(); i++ ) {
            MethodInfo m = methods.get(i);
            if (m.name.equals(method_name) && TypeInference.subtype(args,m.signature.tail()))
                return m.signature.head();
        };
        return null;
    }

    /** return the method number of a system method with a given name over some expressions;
     * When the method is overloaded, find the most specific (in terms of arg subtyping)
     * @param method_name the given method name
     * @param args the method expressions
     * @return the method number
     */
    public static int find_method_number ( String method_name, Trees args ) {
        for (int i = 0; i < methods.size(); i++ ) {
            MethodInfo m = methods.get(i);
            if (m.name.equals(method_name) && TypeInference.subtype(args,m.signature.tail()))
                return i;
        };
        return -1;
    }

    /** call a system method with a given number over MRData
     * @param method_number the method number
     * @param args in input arguments
     * @return the result of invoking this method over the args
     */
    public static MRData call ( int method_number, MRData... args ) {
        if (method_number < 0 || method_number >= methods.size())
            throw new Error("Run-time error (unknown method name)");
        MethodInfo m = methods.get(method_number);
        try {
            return (MRData)m.method.invoke(null,(Object[])args);
        } catch (Exception e) {
            Tuple t = new Tuple(args.length);
            for ( int i = 0; i < args.length; i++ )
                t.set(i,args[i]);
            System.err.println("Run-time error in method call: "+m.name+t+" of type "
                               +m.signature.tail()+"->"+m.signature.head());
            throw new Error(e.toString());
        }
    }
}
