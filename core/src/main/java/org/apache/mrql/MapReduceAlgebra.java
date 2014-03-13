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

import org.apache.mrql.gen.*;
import java.io.*;
import java.util.*;


/** Evaluation of MRQL algebra expressions in memory */
final public class MapReduceAlgebra {

    /** eager concat-map (not used) */
    private static Bag cmap_eager ( final Function f, final Bag s ) {
        Bag res = new Bag();
        for ( MRData e: s )
            res.addAll((Bag)f.eval(e));
        return res;
    }

    /** lazy concat-map (stream-based)
     * @param f a function from a to {b}
     * @param s the input of type {a}
     * @return a value of type {b}
     */
    public static Bag cmap ( final Function f, final Bag s ) {
        final Iterator<MRData> si = s.iterator();
        return new Bag(new BagIterator() {
                Iterator<MRData> data = null;
                boolean more = false;
                public boolean hasNext () {
                    if (data == null) {
                        while (!more && si.hasNext()) {
                            data = ((Bag)f.eval(si.next())).iterator();
                            more = data.hasNext();
                        }
                    } else {
                        if (more) {
                            more = data.hasNext();
                            if (more)
                                return true;
                        };
                        while (!more && si.hasNext()) {
                            data = ((Bag)f.eval(si.next())).iterator();
                            more = data.hasNext();
                        }
                    };
                    return more;
                }
                public MRData next () {
                    return data.next();
                }
            });
    }

    /** lazy map
     * @param f a function from a to b
     * @param s the input of type {a}
     * @return a value of type {b}
     */
    public static Bag map ( final Function f, final Bag s ) {
        final Iterator<MRData> si = s.iterator();
        return new Bag(new BagIterator() {
                public boolean hasNext () { return si.hasNext(); }
                public MRData next () { return f.eval(si.next()); }
            });
    }

    /** lazy filter combined with a map
     * @param p a function from a to boolean
     * @param f a function from a to b
     * @param s the input of type {a}
     * @return a value of type {b}
     */
    public static Bag filter ( final Function p, final Function f, final Bag s ) {
        final Iterator<MRData> si = s.iterator();
        return new Bag(new BagIterator() {
                MRData data = null;
                public boolean hasNext () {
                    while (si.hasNext()) {
                        data = si.next();
                        if (((MR_bool)p.eval(data)).get())
                            return true;
                    };
                    return false;
                }
                public MRData next () { return f.eval(data); }
            });
    }

    /** strict group-by
     * @param s the input of type {(a,b)}
     * @return a value of type {(a,{b})}
     */
    public static Bag groupBy ( Bag s ) {
        Bag res = new Bag();
        s.sort();
        MRData last = null;
        Bag group = new Bag();
        for ( MRData e: s) {
            final Tuple p = (Tuple)e;
            if (last != null && p.first().equals(last))
                group.add(p.second());
            else {
                if (last != null) {
                    group.trim();
                    res.add(new Tuple(last,group));
                };
                last = p.first();
                group = new Bag();
                group.add(p.second());
            }
        };
        if (last != null) {
            group.trim();
            res.add(new Tuple(last,group));
        };
        //res.trim();
        return res;
    }

    /** lazy group-by (not used) */
    private static Bag groupBy_lazy ( Bag s ) {
        s.sort();
        final Iterator<MRData> it = s.iterator();
        return new Bag(new BagIterator() {
                MRData last = null;
                MRData data = null;
                Bag group = new Bag();
                public boolean hasNext () {
                    while (it.hasNext()) {
                        final Tuple p = (Tuple)it.next();
                        if (last != null && p.first().equals(last))
                            group.add(p.second());
                        else if (last != null) {
                            group.trim();
                            data = new Tuple(last,group);
                            last = p.first();
                            group = new Bag();
                            group.add(p.second());
                            return true;
                        } else {
                            last = p.first();
                            group = new Bag();
                            group.add(p.second());
                        }
                    };
                    if (last != null) {
                        group.trim();
                        data = new Tuple(last,group);
                        last = null;
                        return true;
                    };
                    return false;
                }
                public MRData next () {
                    return data;
                }
            });
    }

    /** the MapReduce operation
     * @param m a map function from a to {(k,b)}
     * @param r a reduce function from (k,{b}) to {c}
     * @param s the input of type {a}
     * @return a value of type {c}
     */
    public static Bag mapReduce ( final Function m, final Function r, final Bag s ) {
        return cmap(r,groupBy(cmap(m,s)));
    }

    /** Not used: use mapReduce2 instead */
    private static Bag join ( final Function kx, final Function ky, final Function f,
                              final Bag X, final Bag Y ) {
        return cmap(new Function() {
                public Bag eval ( final MRData e ) {
                    final Tuple p = (Tuple)e;
                    return (Bag)f.eval(new Tuple(p.second(),
                                                 cmap(new Function() {
                                                         public Bag eval ( final MRData y ) {
                                                             return (ky.eval(y).equals(p.first()))
                                                                     ? new Bag(y)
                                                                     : new Bag();
                                                         } }, Y))); }
            },
            groupBy(cmap(new Function() {
                    public Bag eval ( final MRData x ) {
                        return new Bag(new Tuple(kx.eval(x),x));
                    } }, X)));
    }

    /** A hash-based equi-join
     * @param kx left key function from a to k
     * @param ky right key function from b to k
     * @param f reducer from (a,b) to c
     * @param X left input of type {a}
     * @param Y right input of type {b}
     * @return a value of type {c}
     */
    public static Bag hash_join ( final Function kx, final Function ky, final Function f,
                                  final Bag X, final Bag Y ) {
        Hashtable<MRData,Bag> hashTable = new Hashtable<MRData,Bag>(1000);
        for ( MRData x: X ) {
            MRData key = kx.eval(x);
            Bag old = hashTable.get(key);
            if (old == null)
                hashTable.put(key,new Bag(x));
            else old.add(x);
        };
        Bag res = new Bag();
        for ( MRData y: Y ) {
            MRData key = ky.eval(y);
            Bag match = hashTable.get(key);
            if (match != null)
                for ( MRData x: match )
                    res.add(f.eval(new Tuple(x,y)));
        };
        return res;
    }

    /** A cross-product
     * @param mx left map function from a to {a'}
     * @param my right key function from b to {b'}
     * @param r reducer from (a',b') to {c}
     * @param X left input of type {a}
     * @param Y right input of type {b}
     * @return a value of type {c}
     */
    public static Bag crossProduct ( final Function mx, final Function my, final Function r,
                                     final Bag X, final Bag Y ) {
        Bag a = new Bag();
        for ( MRData y: Y )
            for ( MRData v: (Bag)my.eval(y) )
                a.add(v);
        Bag b = new Bag();
        for ( MRData x: X )
            for ( MRData xx: (Bag)mx.eval(x) )
                for ( MRData y: a )
                    for ( MRData v: (Bag)r.eval(new Tuple(xx,y)) )
                        b.add(v);
        return b;
    }

    /** A map-reduce operation with two mappers (a join)
     * @param mx left map function from a to {(k,a')}
     * @param my right key function from b to {(k,b')}
     * @param r reducer from ({a'},{b'}) to {c}
     * @param X left input of type {a}
     * @param Y right input of type {b}
     * @return a value of type {c}
     */
    public static Bag mapReduce2 ( final Function mx,   // left mapper
                                   final Function my,   // right mapper
                                   final Function r,    // reducer
                                   final Bag X, final Bag Y ) {
        final Bag left = cmap(new Function() {
                public Bag eval ( final MRData x ) {
                    return cmap(new Function() {
                            public Bag eval ( final MRData e ) {
                                final Tuple p = (Tuple)e;
                                return new Bag(new Tuple(p.first(),
                                                         new Tuple(new MR_byte(1),p.second())));
                            } }, (Bag)mx.eval(x));
                } }, X);
        final Bag right = cmap(new Function() {
                public Bag eval ( final MRData y ) {
                    return cmap(new Function() {
                            public Bag eval ( final MRData e ) {
                                final Tuple p = (Tuple)e;
                                return new Bag(new Tuple(p.first(),
                                                         new Tuple(new MR_byte(2),p.second())));
                            } }, (Bag)my.eval(y));
                } }, Y);
        final Iterator<MRData> li = left.iterator();
        final Iterator<MRData> ri = right.iterator();
        final Bag mix = new Bag(new BagIterator () {
                MRData data;
                public boolean hasNext () {
                    if (li.hasNext()) {
                        data = li.next();
                        return true;
                    } else if (ri.hasNext()) {
                        data = ri.next();
                        return true;
                    } else return false;
                }
                public MRData next () {
                    return data;
                }
            });
        return cmap(new Function() {
                public Bag eval ( final MRData e ) {
                    final Tuple p = (Tuple)e;
                    final Bag xs = cmap(new Function() {
                            public Bag eval ( final MRData e ) {
                                final Tuple q = (Tuple)e;
                                return (((MR_byte)q.first()).get() == 1)
                                        ? new Bag(q.second())
                                        : new Bag();
                            } }, (Bag)p.second());
                    final Bag ys = cmap(new Function() {
                            public Bag eval ( final MRData e ) {
                                final Tuple q = (Tuple)e;
                                return (((MR_byte)q.first()).get() == 2)
                                        ? new Bag(q.second())
                                        : new Bag();
                            } }, (Bag)p.second());
                    xs.materialize();
                    ys.materialize();
                    return (Bag)r.eval(new Tuple(xs,ys));
                } }, groupBy(mix));
    }

    /** The fragment-replicate join (map-side join)
     * @param kx left key function from a to k
     * @param ky right key function from b to k
     * @param r reducer from (a,{b}) to {c}
     * @param X left input of type {a}
     * @param Y right input of type {b}
     * @return a value of type {c}
     */
    public static Bag mapJoin ( final Function kx, final Function ky, final Function r,
                                final Bag X, final Bag Y ) {
        X.materialize();
        Y.materialize();
        return cmap(new Function() {
                public Bag eval ( final MRData e ) {
                    final Tuple p = (Tuple)e;
                    return cmap(new Function() {
                                    public Bag eval ( final MRData x ) {
                                        return (kx.eval(x).equals(p.first()))
                                            ? (Bag)r.eval(new Tuple(x,p.second()))
                                            : new Bag();
                                    } }, X); }
            },
            groupBy(cmap(new Function() {
                    public Bag eval ( final MRData y ) {
                        return new Bag(new Tuple(ky.eval(y),y));
                    } }, Y)));
    }

    /** An equi-join combined with a group-by (see GroupByJoinPlan)
     * @param kx left key function from a to k
     * @param ky right key function from b to k
     * @param gx group-by key function from a to k1
     * @param gy group-by key function from b to k2
     * @param m mapper from (a,b) to {c}
     * @param c combiner from ((k1,k2),{c}) to d
     * @param r reducer from ((k1,k2),d) to {e}
     * @param X left input of type {a}
     * @param Y right input of type {b}
     * @return a value of type {e}
     */
    public static Bag groupByJoin ( final Function kx, final Function ky,
                                    final Function gx, final Function gy,
                                    final Function m, final Function c, final Function r,
                                    final Bag X, final Bag Y ) {
        Bag s = groupBy(hash_join(kx,ky,
                                  new Function() {
                                      public MRData eval ( final MRData e ) {
                                          Tuple t = (Tuple)e;
                                          return new Tuple(new Tuple(gx.eval(t.first()),gy.eval(t.second())),t);
                                      } },
                                  X,Y));
        Bag res = new Bag();
        for ( MRData z: s ) {
            Tuple t = (Tuple)z;
            for ( MRData n: (Bag)r.eval(new Tuple(t.first(),c.eval(new Tuple(t.first(),cmap(m,(Bag)t.second()))))) )
                res.add(n);
        };
        return res;
    }

    private static void flush_table ( final Map<MRData,MRData> hashTable, final Function r, final Bag result ) {
        Bag tbag = new Bag(2);
        Tuple pair = new Tuple(2);
        for ( Map.Entry<MRData,MRData> entry: hashTable.entrySet() ) {
            pair.set(0,entry.getKey());
            tbag.clear();
            tbag.add_element(entry.getValue());
            pair.set(1,tbag);
            for ( MRData e: (Bag)r.eval(pair) )
                result.add(e);
        };
        hashTable.clear();
    }

    /** An equi-join combined with a group-by implemented using hashing
     * @param kx left key function from a to k
     * @param ky right key function from b to k
     * @param gx group-by key function from a to k1
     * @param gy group-by key function from b to k2
     * @param m mapper from (a,b) to {c}
     * @param c combiner from ((k1,k2),{c}) to d
     * @param r reducer from ((k1,k2),d) to {e}
     * @param X left input of type {a}
     * @param Y right input of type {b}
     * @return a value of type {e}
     */
    final public static Bag mergeGroupByJoin ( final Function kx, final Function ky,
                                               final Function gx, final Function gy,
                                               final Function m, final Function c, final Function r,
                                               Bag X, Bag Y ) {
        Bag tbag = new Bag(2);
        Tuple pair = new Tuple(2);
        Tuple vpair = new Tuple(2);
        final Map<MRData,MRData> hashTable = new HashMap<MRData,MRData>(1000);
        Bag xs = groupBy(map(new Function() {
                public MRData eval ( final MRData e ) {
                    Tuple t = (Tuple)e;
                    return new Tuple(new Tuple(t.first(),kx.eval(t.second())),t.second());
                } }, X));
        Bag ys = groupBy(map(new Function() {
                public MRData eval ( final MRData e ) {
                    Tuple t = (Tuple)e;
                    return new Tuple(new Tuple(t.first(),ky.eval(t.second())),t.second());
                } }, Y));
        X = null; Y = null;
        Bag res = new Bag();
        final Iterator<MRData> xi = xs.iterator();
        final Iterator<MRData> yi = ys.iterator();
        if ( !xi.hasNext() || !yi.hasNext() )
            return res;
        Tuple x = (Tuple)xi.next();
        Tuple y = (Tuple)yi.next();
        MRData partition = null;
        while ( xi.hasNext() && yi.hasNext() ) {
            int cmp = x.first().compareTo(y.first());
            if (cmp < 0) { x = (Tuple)xi.next(); continue; };
            if (cmp > 0) { y = (Tuple)yi.next(); continue; };
            if (partition == null)
                partition = ((Tuple)x.first()).first();
            else if (!partition.equals(((Tuple)x.first()).first())) {
                partition = ((Tuple)x.first()).first();
                flush_table(hashTable,r,res);
            };
            for ( MRData xx: (Bag)x.second() )
                for ( MRData yy: (Bag)y.second() ) {
                    Tuple key = new Tuple(gx.eval(xx),gy.eval(yy));
                    vpair.set(0,xx).set(1,yy);
                    MRData old = hashTable.get(key);
                    pair.set(0,key);
                    for ( MRData e: (Bag)m.eval(vpair) )
                        if (old == null)
                            hashTable.put(key,e);
                        else {
                            tbag.clear();
                            tbag.add_element(e).add_element(old);
                            pair.set(1,tbag);
                            for ( MRData z: (Bag)c.eval(pair) )
                                hashTable.put(key,z);  // normally, done once
                        }
                };
            if (xi.hasNext())
                x = (Tuple)xi.next();
            if (yi.hasNext())
                y = (Tuple)yi.next();
        };
        flush_table(hashTable,r,res);
        return res;
    }

    /** repeat the loop until all termination conditions are true or until we reach the max num of steps
     * @param loop a function from {a} to {(a,boolean)}
     * @param init the initial value of type {a}
     * @param max_num the maximum number of steps
     * @return a value of type {a}
     */
    public static Bag repeat ( final Function loop,
                               final Bag init,
                               final int max_num ) throws Exception {
        boolean cont;
        int i = 0;
        Bag s = init;
        s.materializeAll();
        do {
            MRData d = loop.eval(s);
            i++;
            cont = false;
            if (d instanceof Bag) {
                Bag bag = (Bag) d;
                bag.materialize();
                s.clear();
                for ( MRData x: bag ) {
                    Tuple t = (Tuple)x;
                    cont |= ((MR_bool)t.second()).get();
                    s.add(t.first());
                }
            } else if (d instanceof MR_dataset) {
                DataSet ds = ((MR_dataset)d).dataset();
                if (ds.counter != 0)
                    cont = true;
                System.err.println("*** Repeat #"+i+": "+ds.counter+" true results");
                s = Plan.collect(ds);
            } else throw new Error("Wrong repeat");
        } while (cont && i <= max_num);
        return s;
    }

    /** transitive closure: repeat the loop until the new set is equal to the previous set
     *    or until we reach the max num of steps
     * @param loop a function from {a} to {a}
     * @param init the initial value of type {a}
     * @param max_num the maximum number of steps
     * @return a value of type {a}
     */
    public static Bag closure ( final Function loop,
                                final Bag init,
                                final int max_num ) throws Exception {
        int i = 0;
        long n = 0;
        long old = 0;
        Bag s = init;
        s.materializeAll();
        do {
            MRData d = loop.eval(s);
            i++;
            if (d instanceof Bag) {
                s = (Bag)d;
                s.materialize();
                old = n;
                n = s.size();
            } else if (d instanceof MR_dataset) {
                DataSet ds = ((MR_dataset)d).dataset();
                System.err.println("*** Repeat #"+i+": "+(ds.records-n)+" new records");
                old = n;
                n = ds.records;
                s = Plan.collect(ds);
            } else throw new Error("Wrong repeat");
        } while (old < n && i <= max_num);
        return s;
    }

    /** parse a text document using a given parser
     * @param parser the parser
     * @param file the text document (local file)
     * @param args the arguments to pass to the parser
     * @return a lazy bag that contains the parsed data
     */
    public static Bag parsedSource ( final Parser parser,
                                     final String file,
                                     Trees args ) {
        try {
            parser.initialize(args);
            parser.open(file);
            return new Bag(new BagIterator() {
                    Iterator<MRData> result = null;
                    MRData data;
                    public boolean hasNext () {
                        try {
                            while (result == null || !result.hasNext()) {
                                String s = parser.slice();
                                if (s == null)
                                    return false;
                                result = parser.parse(s).iterator();
                            };
                            data = (MRData)result.next();
                            return true;
                        } catch (Exception e) {
                            throw new Error(e);
                        }
                    }
                    public MRData next () {
                        return data;
                    }
                });
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    /** parse a text document using a given parser
     * @param parser the name of the parser
     * @param file the text document (local file)
     * @param args the arguments to pass to the parser
     * @return a lazy bag that contains the parsed data
     */
    public static Bag parsedSource ( String parser, String file, Trees args ) {
        try {
            return parsedSource(DataSource.parserDirectory.get(parser).newInstance(),file,args);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private static Bag add_source_num ( int source_num, Bag input ) {
        return new Bag(new Tuple(new MR_int(source_num),input));
    }

    /** parse a text document using a given parser and tag output data with a source num
     * @param source_num the source id
     * @param parser the parser
     * @param file the text document (local file)
     * @param args the arguments to pass to the parser
     * @return a lazy bag that contains the parsed data taged with the source id
     */
    public static Bag parsedSource ( int source_num,
                                     Parser parser,
                                     String file,
                                     Trees args ) {
        return add_source_num(source_num,parsedSource(parser,file,args));
    }

    /** parse a text document using a given parser and tag output data with a source num
     * @param source_num the source id
     * @param parser the name of the parser
     * @param file the text document (local file)
     * @param args the arguments to pass to the parser
     * @return a lazy bag that contains the parsed data taged with the source id
     */
    public static Bag parsedSource ( int source_num, String parser, String file, Trees args ) {
        try {
            return parsedSource(source_num,DataSource.parserDirectory.get(parser).newInstance(),file,args);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    /** aggregate the Bag elements
     * @param accumulator a function from (b,a) to b
     * @param zero a value of type b
     * @param s a Bag of type {a}
     * @return a value of type b
     */
    public static MRData aggregate ( final Function accumulator,
                                     final MRData zero,
                                     final Bag s ) {
        MRData result = zero;
        for ( MRData x: s )
            result = accumulator.eval(new Tuple(result,x));
        return result;
    }

    public static MRData materialize ( MRData x ) {
        if (x instanceof Bag)
            ((Bag)x).materialize();
        return x;
    }

    /** Dump the value of some type to a binary local file;
     *  The type is dumped to a separate file.type
     */
    public static void dump ( String file, Tree type, MRData value ) throws IOException {
        PrintStream ftp = new PrintStream(file+".type");
        ftp.print("1@"+type.toString()+"\n");
        ftp.close();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(file)));
        value.write(out);
        out.close();
    }

    /** return the type of the dumped binary local file from file.type */
    public static Tree get_type ( String file ) {
        try {
            BufferedReader ftp = new BufferedReader(new FileReader(new File(file+".type")));
            String s[] = ftp.readLine().split("@");
            ftp.close();
            if (s.length != 2)
                return null;
            if (!s[0].equals("1"))
                throw new Error("The binary file has been created in hadoop mode and cannot be read in java mode");
            return Tree.parse(s[1]);
        } catch (Exception e) {
            return null;
        }
    }

    /** read the contents of a dumped local binary file */
    public static MRData read_binary ( String file ) {
        try {
            Tree type = get_type(file);
            DataInputStream in = new DataInputStream(new FileInputStream(new File(file)));
            return MRContainer.read(in);
        } catch (Exception e) {
            return null;
        } 
    }

    /** read the contents of a dumped local binary file and tag data with a source num */
    public static Bag read_binary ( int source_num, String file ) {
        return add_source_num(source_num,(Bag)read_binary(file));
    }

    /** generate a lazy bag of long numbers {min...max} */
    public static Bag generator ( final long min, final long max ) {
        if (min > max)
            throw new Error("Min value ("+min+") is larger than max ("+max+") in generator");
        return new Bag(new BagIterator() {
                long index = min;
                public boolean hasNext () {
                    return index <= max;
                }
                public MRData next () {
                    return new MR_long(index++);
                }
            });
    }

    /** generate a lazy bag of long numbers {min...max} and tag each lon number with a source num */
    public static Bag generator ( int source_num, final long min, final long max ) {
        return add_source_num(source_num,generator(min,max));
    }

    /** the cache that holds all local data in memory */
    private static Tuple cache;

    /** return the cache element at location loc */
    public static MRData getCache ( int loc ) {
        return cache.get(loc);
    }

    /** set the cache element at location loc to value and return ret */
    public static MRData setCache ( int loc, MRData value, MRData ret ) {
        if (value instanceof Bag)
            materialize((Bag)value);
        cache.set(loc,value);
        return ret;
    }

    /** The BSP operation
     * @param source the source ids of the input Bags
     * @param superstep the BSP superstep is a function from ({M},S) to ({M},S,boolean)
     * @param init_state is the initial state of type S
     * @param order do we need to order the result?
     * @param inputs the input Bags
     * @return return a Bag in cache[0]
     */
    public static MRData BSP ( final int[] source,
                               final Function superstep,
                               final MRData init_state,
                               boolean order,
                               final Bag[] inputs ) {
        Bag msgs = new Bag();
        MRData state = init_state;
        Tuple result;
        boolean exit;
        boolean skip = false;
        String tabs = "";
        int step = 0;
        cache = new Tuple(100);
        for ( int i = 0; i < 100; i++ )
            cache.set(i,new Bag());
        for ( Bag x: inputs ) {
            Tuple p = (Tuple)(x.get(0));
            cache.set(((MR_int)p.first()).get(),
                      materialize(p.second()));
        };
        do {
            if (!skip)
                step++;
            if (!skip && Config.trace_execution) {
                tabs = Interpreter.tabs(Interpreter.tab_count);
                System.out.println(tabs+"  Superstep "+step+":");
                System.out.println(tabs+"      messages: "+msgs);
                System.out.println(tabs+"      state: "+state);
                for ( int i = 0; i < cache.size(); i++)
                    if (cache.get(i) instanceof Bag && ((Bag)cache.get(i)).size() > 0)
                        System.out.println(tabs+"      cache "+i+": "+cache.get(i));
            };
            result = (Tuple)superstep.eval(new Tuple(cache,msgs,state,new MR_string("")));
            Bag new_msgs = (Bag)result.get(0);
            state = result.get(1);
            exit = ((MR_bool)result.get(2)).get();
            skip = new_msgs == SystemFunctions.bsp_empty_bag;
            if ((!skip || exit) && Config.trace_execution)
                System.out.println(tabs+"      result: "+result);
            final Iterator<MRData> iter = new_msgs.iterator();
            msgs = new Bag(new BagIterator() {
                    public boolean hasNext () {
                        return iter.hasNext();
                    }
                    public MRData next () {
                        return ((Tuple)iter.next()).get(1);
                    }
                });
        } while (!exit);
        MRData[] data = new MRData[source.length];
        for ( int i = 0; i < data.length; i++ )
            data[i] = getCache(source[i]);
        if (order && data[0] instanceof Bag) {
            final Iterator<MRData> iter = ((Bag)data[0]).iterator();
            return new Bag(new BagIterator() {
                    public boolean hasNext () {
                        return iter.hasNext();
                    }
                    public MRData next () {
                        return ((Tuple)iter.next()).get(0);
                    }
                });
        };
        if (data.length == 1)
            return data[0];
        else return new Tuple(data);
    }
}
