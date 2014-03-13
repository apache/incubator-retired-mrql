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

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;


/**
 *   A sequence of MRData.
 *   There are 3 kinds of Bag implementations, which are converted at run-time, when necessary:
 *   1) vector-based (materialized): used for small bags (when size is less than Config.max_materialized_bag);
 *   2) stream-based: can be traversed only once; implemented as Java iterators;
 *   3) spilled to a local file: can be accessed multiple times
 */
public class Bag extends MRData implements Iterable<MRData> {
    private final static long serialVersionUID = 64629834894869L;
    enum Modes { STREAMED, MATERIALIZED, SPILLED };
    private transient Modes mode;
    private transient ArrayList<MRData> content;      // content of a materialized bag
    private transient BagIterator iterator;           // iterator for a streamed bag
    private transient boolean consumed;               // true, if the stream has already been used
    private transient String path;                    // local path that contains the spilled bag
    private transient SequenceFile.Writer writer;     // the file writer for spiled bags

    /**
     * create an empty bag as an ArrayList
     */
    public Bag () {
        mode = Modes.MATERIALIZED;
        content = new ArrayList<MRData>();
    }

    /**
     * create an empty bag as an ArrayList with a given capacity
     * @param size initial capacity
     */
    public Bag ( final int size ) {
        mode = Modes.MATERIALIZED;
        content = new ArrayList<MRData>(size);
    }

    /**
     * in-memory Bag construction (an ArrayList) initialized with data
     * @param as a vector of MRData to insert in the Bag
     */
    public Bag ( final MRData ...as ) {
        mode = Modes.MATERIALIZED;
        content = new ArrayList<MRData>(as.length);
        for ( MRData a: as )
            content.add(a);
    }

    /**
     * in-memory Bag construction (an ArrayList) initialized with data
     * @param as a vector of MRData to insert in the Bag
     */
    public Bag ( final List<MRData> as ) {
        mode = Modes.MATERIALIZED;
        content = new ArrayList<MRData>(as.size());
        for ( MRData a: as )
            content.add(a);
    }

    /**
     * lazy construction (stream-based) of a Bag
     * @param i the Iterator that generates the Bag elements
     */
    public Bag ( final BagIterator i ) {
        mode = Modes.STREAMED;
        iterator = i;
        consumed = false;
    }

    /** is the Bag stored in an ArrayList? */
    public boolean materialized () {
        return mode == Modes.MATERIALIZED;
    }

    /** is the Bag stream-based? */
    public boolean streamed () {
        return mode == Modes.STREAMED;
    }

    /** is the Bag spilled into a file? */
    public boolean spilled () {
        return mode == Modes.SPILLED;
    }

    /** return the Bag size (cache it in memory if necessary) */
    public int size () {
        if (materialized())
            return content.size();
        if (streamed() && consumed)
            throw new Error("*** The collection stream has already been consumed");
        int i = 0;
        for ( MRData e: this )
            i++;
        if (streamed())
            consumed = true;
        return i;
    }

    /** trim the ArrayList that caches the Bag */
    public void trim () {
        if (materialized())
            content.trimToSize();
    }

    /** get the n'th element of a Bag (cache it in memory if necessary)
     * @param n the index
     * @return the n'th element
     */
    public MRData get ( final int n ) {
        if (materialized())
            if (n < size())
                return content.get(n);
            else throw new Error("List index out of range: "+n);
        if (streamed() && consumed)
            throw new Error("*** The collection stream has already been consumed");
        int i = 0;
        for ( MRData e: this )
            if (i++ == n)
                return e;
        if (streamed())
            consumed = true;
        throw new Error("Cannot retrieve the "+n+"th element of a sequence");
    }

    /** replace the n'th element of a Bag with a new value
     * @param n the index
     * @param value the new value
     * @return the Bag
     */
    public Bag set ( final int n, final MRData value ) {
        if (!materialized())
            throw new Error("Cannot replace an element of a non-materialized sequence");
        content.set(n,value);
        return this;
    }

    /** add a new value to a Bag (cache it in memory if necessary)
     * @param x the new value
     */
    public void add ( final MRData x ) {
        materialize();
        if (!spilled() && Config.hadoop_mode
             && size() >= Config.max_materialized_bag)
            spill();
        if (spilled())
            try {
                if (writer == null) {   // writer was closed earlier for reading
                    FileSystem fs = FileSystem.getLocal(Plan.conf);
                    writer = SequenceFile.createWriter(fs,Plan.conf,new Path(path),
                                                       MRContainer.class,NullWritable.class,
                                                       SequenceFile.CompressionType.NONE);
                    System.err.println("*** Appending elements to a spilled Bag: "+path);
                };
                writer.append(new MRContainer(x),NullWritable.get());
            } catch (IOException e) {
                throw new Error("Cannot append an element to a spilled Bag: "+path);
            }
        else content.add(x);
    }

    /** add a new value to a Bag (cache it in memory if necessary)
     * @param x the new value
     * @return the Bag
     */
    public Bag add_element ( final MRData x ) {
        add(x);
        return this;
    }

    /** add the elements of a Bag to the end of this Bag
     * @param b the Bag whose elements are copied
     * @return the Bag
     */
    public Bag addAll ( final Bag b ) {
        for ( MRData e: b )
            add(e);
        return this;
    }

    /** make this Bag empty (cache it in memory if necessary) */
    public void clear () {
        if (materialized())
            content.clear();
        else if (streamed()) {
            if (writer != null)
                try {
                    writer.close();
                } catch (IOException ex) {
                    throw new Error(ex);
                };
            writer = null;
            path = null;
            mode = Modes.MATERIALIZED;
            content = new ArrayList<MRData>(100);
        };
        mode = Modes.MATERIALIZED;
        content = new ArrayList<MRData>();
    }

    /** cache the Bag to an ArrayList when is absolutely necessary */
    public void materialize () {
        if (materialized() || spilled())
            return;
        Iterator<MRData> iter = iterator();
        mode = Modes.MATERIALIZED;
        writer = null;
        path = null;
        content = new ArrayList<MRData>(100);
        while ( iter.hasNext() )
            add(iter.next());
        if (materialized())    // it may have been spilled
            content.trimToSize();
        iterator = null;
    }

    private static Random random_generator = new Random();

    private static String new_path ( FileSystem fs ) throws IOException {
        Path p;
        do {
            p = new Path("file://"+Config.tmpDirectory+"/mrql"+(random_generator.nextInt(1000000)));
        } while (p.getFileSystem(Plan.conf).exists(p));
        String path = p.toString();
        Plan.temporary_paths.add(path);
        return path;
    }

    /** spill the Bag to a local file */
    private void spill () {
        if (!spilled() && Config.hadoop_mode)
            try {
                if (Plan.conf == null)
                    Plan.conf = Evaluator.evaluator.new_configuration();
                final FileSystem fs = FileSystem.getLocal(Plan.conf);
                path = new_path(fs);
                System.err.println("*** Spilling a Bag to a local file: "+path);
                final Path p = new Path(path);
                writer = SequenceFile.createWriter(fs,Plan.conf,new Path(path),
                                                   MRContainer.class,NullWritable.class,
                                                   SequenceFile.CompressionType.NONE);
                for ( MRData e: this )
                    writer.append(new MRContainer(e),NullWritable.get());
                mode = Modes.SPILLED;
                content = null;
                iterator = null;
            } catch (Exception e) {
                throw new Error("Cannot spill a Bag to a local file");
            }
    }

    /**
     * sort the Bag (cache it in memory if necessary).
     * If the Bag was spilled during caching, use external sorting
     */
    public void sort () {
        materialize();
        if (spilled())  // if it was spilled during materialize()
            try {       // use external sorting
                if (writer != null)
                    writer.close();
                FileSystem fs = FileSystem.getLocal(Plan.conf);
                SequenceFile.Sorter sorter
                    = new SequenceFile.Sorter(fs,new Plan.MRContainerKeyComparator(),
                                              MRContainer.class,NullWritable.class,Plan.conf);
                String out_path = new_path(fs);
                System.err.println("*** Using external sorting on a spilled bag "+path+" -> "+out_path);
                sorter.setMemory(64*1024*1024);
                sorter.sort(new Path(path),new Path(out_path));
                path = out_path;
                writer = null;
            } catch (Exception ex) {
                throw new Error("Cannot sort a spilled bag");
            }
        else Collections.sort(content);
    }

    /** return the Bag Iterator */
    public Iterator<MRData> iterator () {
        if (spilled())
            try {
                if (writer != null)
                    writer.close();
                writer = null;
                return new BagIterator () {
                    final FileSystem fs = FileSystem.getLocal(Plan.conf);
                    final SequenceFile.Reader reader = new SequenceFile.Reader(fs,new Path(path),Plan.conf);
                    final MRContainer key = new MRContainer();
                    final NullWritable value = NullWritable.get();
                    MRData data;
                    public boolean hasNext () {
                        try {
                            if (!reader.next(key,value)) {
                                reader.close();
                                return false;
                            };
                            data = key.data();
                            return true;
                        } catch (IOException e) {
                            throw new Error("Cannot collect values from a spilled Bag");
                        }
                    }
                    public MRData next () {
                        return data;
                    }
                };
            } catch (IOException e) {
                throw new Error("Cannot collect values from a spilled Bag");
            }
        else if (materialized())
            return content.iterator();
        else {
            if (consumed)  // this should never happen
                throw new Error("*** The collection stream has already been consumed");
            consumed = true;
            return iterator;
        }
    }

    /** cache MRData in memory by caching all Bags at any place and depth in MRData */
    public void materializeAll () {
        materialize();
        for (MRData e: this)
            e.materializeAll();
    }

    /** concatenate the elements of a given Bag to the elements of this Bag.
     * Does not change either Bag
     * @param s the given Bag
     * @return a new Bag
     */
    public Bag union ( final Bag s ) {
        final Iterator<MRData> i1 = iterator();
        final Iterator<MRData> i2 = s.iterator();
        return new Bag(new BagIterator () {
                boolean first = true;
                public boolean hasNext () {
                    if (first)
                        if (i1.hasNext())
                            return true;
                        else {
                            first = false;
                            return i2.hasNext();
                        }
                    else return i2.hasNext();
                }
                public MRData next () {
                    if (first)
                        return i1.next();
                    else return i2.next();
                }
            });
    }

    /** does this Bag contain an element?
     * Cache this Bag in memory befor tetsing if necessary
     * @param x the element to find
     */
    public boolean contains ( final MRData x ) {
        if (materialized())
            return content.contains(x);
        if (streamed() && consumed)
            throw new Error("*** The collection stream has already been consumed");
        for ( MRData e: this )
            if (x.equals(e))
                return true;
        if (streamed())
            consumed = true;
        return false;
    }

    /** if this Bag is a Map from keys to values (a Bag of (key,value) pairs),
     * find the value with the given key; raise an error if not found
     * @param key the search key
     * @return the value associated with the key
     */
    public MRData map_find ( final MRData key ) {
        if (streamed() && consumed)
            throw new Error("*** The collection stream has already been consumed");
        for ( MRData e: this ) {
            Tuple p = (Tuple) e;
            if (key.equals(p.first()))
                return p.second();
        };
        if (streamed())
            consumed = true;
        throw new Error("key "+key+" not found in map");
    }

    /** if this Bag is a Map from keys to values (a Bag of (key,value) pairs),
     * does it contain a given key?
     * @param key the search key
     */
    public boolean map_contains ( final MRData key ) {
        if (streamed() && consumed)
            throw new Error("*** The collection stream has already been consumed");
        for ( MRData e: this )
            if (key.equals(((Tuple)e).first()))
                return true;
        if (streamed())
            consumed = true;
        return false;
    }

    /** the output serializer for Bag.
     * Stream-based Bags are serialized lazily (without having to cache the Bag in memory)
     */
    final public void write ( DataOutput out ) throws IOException {
        if (materialized()) {
            out.writeByte(MRContainer.BAG);
            WritableUtils.writeVInt(out,size());
            for ( MRData e: this )
                e.write(out);
        } else {
            out.writeByte(MRContainer.LAZY_BAG);
            for ( MRData e: this )
                e.write(out);
            out.writeByte(MRContainer.END_OF_LAZY_BAG);
        }
    }

    /** the input serializer for Bag */
    final public static Bag read ( DataInput in ) throws IOException {
        int n = WritableUtils.readVInt(in);
        Bag bag = new Bag(n);
        for ( int i = 0; i < n; i++ )
            bag.add(MRContainer.read(in));
        return bag;
    }

    /** a lazy input serializer for a Bag (it doesn't need to cache a Bag in memory) */
    public static Bag lazy_read ( final DataInput in ) throws IOException {
        Bag bag = new Bag(100);
        MRData data = MRContainer.read(in);
        while (data != MRContainer.end_of_lazy_bag) {
            bag.add(data);
            data = MRContainer.read(in);
        };
        if (bag.materialized())
            bag.content.trimToSize();
        return bag;
    }

    /** the input serializer for Bag */
    public void readFields ( DataInput in ) throws IOException {
        int n = WritableUtils.readVInt(in);
        mode = Modes.MATERIALIZED;
        iterator = null;
        path = null;
        writer = null;
        if (content == null)
            content = new ArrayList<MRData>(n);
        else {
            content.clear();
            content.ensureCapacity(n);
        };
        for ( int i = 0; i < n; i++ )
            add(MRContainer.read(in));
    }

    private void writeObject ( ObjectOutputStream out ) throws IOException {
        materialize();
        WritableUtils.writeVInt(out,size());
        for ( MRData e: this )
            e.write(out);
    }

    private void readObject ( ObjectInputStream in ) throws IOException, ClassNotFoundException {
        int n = WritableUtils.readVInt(in);
        mode = Modes.MATERIALIZED;
        iterator = null;
        path = null;
        writer = null;
        content = new ArrayList<MRData>(n);
        for ( int i = 0; i < n; i++ )
            add(MRContainer.read(in));
    }

    private void readObjectNoData () throws ObjectStreamException { };

    /** compare this Bag with a given Bag by comparing their associated elements */
    public int compareTo ( MRData x ) {
        Bag xt = (Bag)x;
        Iterator<MRData> xi = xt.iterator();
        Iterator<MRData> yi = iterator();
        while ( xi.hasNext() && yi.hasNext() ) {
            int c = xi.next().compareTo(yi.next());
            if (c < 0)
                return -1;
            else if (c > 0)
                return 1;
        };
        if (xi.hasNext())
            return -1;
        else if (yi.hasNext())
            return 1;
        else return 0;
    }

    /** compare this Bag with a given Bag by comparing their associated elements */
    final public static int compare ( byte[] x, int xs, int xl, byte[] y, int ys, int yl, int[] size ) {
        try {
            int xn = WritableComparator.readVInt(x,xs);
            int xx = WritableUtils.decodeVIntSize(x[xs]);
            int yn = WritableComparator.readVInt(y,ys);
            int yy = WritableUtils.decodeVIntSize(y[ys]);
            for ( int i = 0; i < xn && i < yn; i++ ) {
                int k = MRContainer.compare(x,xs+xx,xl-xx,y,ys+yy,yl-yy,size);
                if (k != 0)
                    return k;
                xx += size[0];
                yy += size[0];
            };
            size[0] = xx+1;
            if (xn > yn)
                return 1;
            if (xn < yn)
                return -1;
            return 0;
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    /** is this Bag equal to another Bag (order is important) */
    public boolean equals ( Object x ) {
        if (!(x instanceof Bag))
            return false;
        Bag xt = (Bag) x;
        Iterator<MRData> xi = xt.iterator();
        Iterator<MRData> yi = iterator();
        while ( xi.hasNext() && yi.hasNext() )
            if ( !xi.next().equals(yi.next()) )
                return false;
        return xi.hasNext() || yi.hasNext();
    }

    /** the hash code of this Bag is the XOR of the hash code of its elements */
    public int hashCode () {
        int h = 127;
        for ( MRData e: this )
            h ^= e.hashCode();
        return Math.abs(h);
    }

    /** show the first few Bag elements (controlled by -bag_print) */
    public String toString () {
        materialize();
        StringBuffer b = new StringBuffer("{ ");
        int i = 0;
        for ( MRData e: this )
            if ( i++ < Config.max_bag_size_print )
                b.append(((i>1)?", ":"")+e);
            else return b.append(", ... }").toString();
        return b.append(" }").toString();
    }
}
