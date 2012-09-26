/********************************************************************************
   Copyright 2011-2012 Leonidas Fegaras, University of Texas at Arlington

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   File: MapReduceAlgebra.java
   The Java algebraic operators for MRQL
   Programmer: Leonidas Fegaras, UTA
   Date: 10/14/10 - 08/15/12

********************************************************************************/

package hadoop.mrql;

import Gen.*;
import java.io.*;
import java.util.*;


final public class MapReduceAlgebra {

    // eager cmap (not used)
    public static Bag cmap_eager ( final Function f, final Bag s ) {
	Bag res = new Bag();
	for ( MRData e: s )
	    res.addAll((Bag)f.eval(e));
	return res;
    }

    // lazy cmap (stream-based)
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

    public static Bag map ( final Function f, final Bag s ) {
	final Iterator<MRData> si = s.iterator();
	return new Bag(new BagIterator() {
		public boolean hasNext () { return si.hasNext(); }
		public MRData next () { return f.eval(si.next()); }
	    });
    }

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

    // strict group-by
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
	res.trim();
	return res;
    }

    public static Bag groupBy_lazy ( Bag s ) {
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

    public static Bag mapReduce ( final Function m, final Function r, final Bag s ) {
	return cmap(r,groupBy(cmap(m,s)));
    }

    // Not used: use mapReduce2 instead
    public static Bag join ( final Function kx, final Function ky, final Function f,
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

    // A map-reduce operation with two mappers (a join)
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

    // The fragment-replicate join (map-side join)
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

    // repetition
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

    // transitive closure
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

    public static Bag parsedSource ( String parser, String file, Trees args ) {
	try {
	    return parsedSource(DataSource.parserDirectory.get(parser).newInstance(),file,args);
	} catch (Exception e) {
	    throw new Error(e);
	}
    }

    private static Bag add_source_num ( final int source_num, Bag input ) {
	final Iterator<MRData> iter = input.iterator();
	final MRData sn = new MR_int(source_num);
	return new Bag(new BagIterator() {
		public boolean hasNext () {
		    return iter.hasNext();
		}
		public MRData next () {
		    return new Tuple(sn,(MRData)iter.next());
		}
	    });
    }

    public static Bag parsedSource ( int source_num,
				     Parser parser,
				     String file,
				     Trees args ) {
	return add_source_num(source_num,parsedSource(parser,file,args));
    }

    public static Bag parsedSource ( int source_num, String parser, String file, Trees args ) {
	try {
	    return parsedSource(source_num,DataSource.parserDirectory.get(parser).newInstance(),file,args);
	} catch (Exception e) {
	    throw new Error(e);
	}
    }

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
    };

    public static void dump ( String file, Tree type, MRData value ) throws IOException {
	PrintStream ftp = new PrintStream(file+".type");
	ftp.print("1@"+type.toString()+"\n");
	ftp.close();
	DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(file)));
	value.write(out);
	out.close();
    }

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

    public static MRData read_binary ( String file ) {
	try {
	    Tree type = get_type(file);
	    DataInputStream in = new DataInputStream(new FileInputStream(new File(file)));
	    return MRContainer.read(in);
	} catch (Exception e) {
	    return null;
	} 
    }

    public static Bag read_binary ( int source_num, String file ) {
	return add_source_num(source_num,(Bag)read_binary(file));
    }

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

    public static Bag generator ( int source_num, final long min, final long max ) {
	return add_source_num(source_num,generator(min,max));
    }

    // The BSP operator
    public static Bag BSP ( final int source,
			    final Function superstep,
			    final MRData init_state,
			    final Bag[] inputs ) {
	Bag input = inputs[0];
	for ( int i = 1; i < inputs.length; i++ )
	    input = input.union(inputs[i]);
	input.materialize();
	Bag msgs = new Bag();
	MRData snapshot = input;
	MRData state = init_state;
	Tuple result;
	boolean exit;
	boolean skip = false;
	String tabs = "";
	int step = 0;
	do {
	    if (!skip)
		step++;
	    if (!skip && Config.trace_execution) {
		tabs = Interpreter.tabs(Interpreter.tab_count);
		System.out.println(tabs+"  Superstep "+step+":");
		System.out.println(tabs+"      messages: "+msgs);
		System.out.println(tabs+"      snapshot: "+snapshot);
		System.out.println(tabs+"      state: "+state);
	    };
	    result = (Tuple)superstep.eval(new Tuple(msgs,snapshot,state));
	    Bag new_msgs = (Bag)result.get(0);
	    snapshot = result.get(1);
	    state = result.get(2);
	    exit = ((MR_bool)result.get(3)).get();
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
	if (snapshot instanceof Bag)
	    return (Bag)snapshot;
	else return new Bag(snapshot);
    }
}
