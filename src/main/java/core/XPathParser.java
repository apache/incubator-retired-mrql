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
import java.util.Stack;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.AttributesImpl;
import java.io.PrintStream;
import java.nio.ByteBuffer;


/** Compiles XPath queries to SAX pipelines */
final public class XPathParser {
    public MRDataHandler dataConstructor;
    public XPathHandler handler;
    final static PrintStream out = System.out;
    static int cached_events = 0;

    /** a SAX handler to form XPath pipelines */
    abstract class XPathHandler extends DefaultHandler {
        XPathHandler next;         // the next handler in the pipeline

        public XPathHandler ( XPathHandler next ) {
            this.next = next;
        }

        public void startDocument () throws SAXException {
            next.startDocument();
        }

        public void endDocument () throws SAXException {
            next.endDocument();
        }

        abstract public void startElement ( String uri, String name, String tag, Attributes atts ) throws SAXException;

        abstract public void endElement ( String uri, String name, String tag ) throws SAXException;

        abstract public void characters ( char text[], int start, int length ) throws SAXException;

        /** start a new predicate */
        public void startPredicate ( int pred ) {
            next.startPredicate(pred);
        }

        /** the end of a predicate */
        public void endPredicate ( int pred ) {
            next.endPredicate(pred);
        }

        public void releasePredicate ( int pred ) {     // set the predicate outcome to true
            next.releasePredicate(pred);
        }
    }

    /** The end of the pipeline: Print the SAX stream to the output */
    final class Print extends XPathHandler {

        public Print () {
            super(null);
        }

        public void startDocument () {}

        public void endDocument () { out.println(); }

        public void startElement ( String uri, String name, String tag, Attributes atts ) {
            out.append("<").append(tag);
            if (atts != null)
                for (int i = 0; i < atts.getLength(); i++)
                    out.append(" ").append(atts.getQName(i))
                        .append("=\"").append(atts.getValue(i)).append("\"");
            out.append(">");
        }

        public void endElement ( String uri, String name, String tag ) {
            out.append("</").append(tag).append(">");
        }

        public void characters ( char text[], int start, int length ) {
            for (int i = 0; i < length; i++)
                out.append(text[start+i]);
        }

        public String toString () { return "print()"; }
    }

    /** A growable buffer for storing events as byte sequences */
    final class Cache {
        final static int buffer_size_increment = 1000;
        public byte[] wrapped_buffer;
        public ByteBuffer byteBuffer;

        public Cache () {
            wrapped_buffer = new byte[buffer_size_increment];
            byteBuffer = ByteBuffer.wrap(wrapped_buffer);
        }

        private void grow ( int len ) {
            len /= java.lang.Byte.SIZE;
            while (len+byteBuffer.position() > byteBuffer.limit()) {
                int pos = byteBuffer.position();
                byte[] nb = new byte[wrapped_buffer.length+buffer_size_increment];
                for (int i = 0; i < wrapped_buffer.length; i++)
                    nb[i] = wrapped_buffer[i];
                wrapped_buffer = nb;
                byteBuffer = ByteBuffer.wrap(nb);
                byteBuffer.position(pos);
            }
        }

        public void putByte ( byte n ) {
            grow(8);
            byteBuffer.put(n);
        }

        public void putShort ( short n ) {
            grow(Short.SIZE);
            byteBuffer.putShort(n);
        }

        public void putInt ( int n ) {
            grow(Integer.SIZE);
            byteBuffer.putInt(n);
        }

        public void putChars ( char text[], int start, int len ) {
            grow(len*Character.SIZE+Integer.SIZE);
            byteBuffer.putInt(len);
            for (int i = 0; i < len; i++)
                byteBuffer.putChar(text[start+i]);
        }

        public void putString ( String s ) {
            grow(s.length()*Character.SIZE+Short.SIZE);
            int len = s.length();
            byteBuffer.putShort((short) len);
            for (int i = 0; i < len; i++)
                byteBuffer.putChar(s.charAt(i));
        }

        public byte getByte () { return byteBuffer.get(); }

        public short getShort () { return byteBuffer.getShort(); }

        public int getInt () { return byteBuffer.getInt(); }

        public char[] getChars () {
            int len = byteBuffer.getInt();
            char[] buf = new char[len];
            for (int i = 0; i < len; i++)
                buf[i] = byteBuffer.getChar();
            return buf;
        }

        public String getString () {
            int len = byteBuffer.getShort();
            char[] buf = new char[len];
            for (int i = 0; i < len; i++)
                buf[i] = byteBuffer.getChar();
            return new String(buf);
        }

        public void cacheStartElement ( String uri, String name, String tag, Attributes atts ) {
            cached_events++;
            putByte((byte)0);
            putString(uri);
            putString(name);
            putString(tag);
            if (atts != null) {
                putShort((short) atts.getLength());
                for (int i = 0; i < atts.getLength(); i++) {
                    putString(atts.getQName(i));
                    putString(atts.getValue(i));
                }
            } else putShort((short) 0);
        }

        public void cacheEndElement ( String uri, String name, String tag ) {
            cached_events++;
            putByte((byte)1);
            putString(uri);
            putString(name);
            putString(tag);
        }

        public void cacheCharacters ( char text[], int start, int length ) {
            cached_events++;
            putByte((byte)2);
            putChars(text,start,length);
        }

        public void print () {
            System.out.println(byteBuffer);
            dump(new Print());
        }

        public void append ( Cache cache ) {
            grow(cache.byteBuffer.position());
            byte[] b = cache.byteBuffer.array();
            byteBuffer.put(b,0,cache.byteBuffer.position());
            cache.byteBuffer.clear();
        }

        /** regenerate the stream from buffer */
        public void dump ( XPathHandler next ) {
            int last = byteBuffer.position();
            byteBuffer.position(0);
            while (byteBuffer.position() < last)
                try {
                    switch (getByte()) {
                    case 0:
                        String uri = getString();
                        String name = getString();
                        String tag = getString();
                        AttributesImpl atts = new AttributesImpl();
                        int len = getShort();
                        for (int i = 0; i < len; i++)
                            atts.addAttribute("","",getString(),"",getString());
                        next.startElement(uri,name,tag,atts);
                        break;
                    case 1:
                        next.endElement(getString(),getString(),getString());
                        break;
                    case 2:
                        char[] text = getChars();
                        next.characters(text,0,text.length);
                    }
                } catch (SAXException e) {
                    throw new Error(e);
                };
            byteBuffer.clear();
        }
    }

    /** Remove the start/end/releasePredicate events by storing some events in a buffer, when necessary */
    final class Materialize extends XPathHandler {
        final static int max_num_of_nested_predicates = 100;
        final public Cache cache;    // nested suspended events from predicates whose outcome is unknown
        final int[] ids;             // the ids of the predicates with suspended output
        final int[] positions;       // position of predicate events in the buffer
        final boolean[] released;    // true if the associated predicate is true
        int top;                     // top of the stacks

        public Materialize ( XPathHandler next ) {
            super(next);
            cache = new Cache();
            ids = new int[max_num_of_nested_predicates];
            positions = new int[max_num_of_nested_predicates];
            released = new boolean[max_num_of_nested_predicates];
            top = 0;
        }

        public void startElement ( String uri, String name, String tag, Attributes atts ) throws SAXException {
            if (top > 0)
                cache.cacheStartElement(uri,name,tag,atts);
            else next.startElement(uri,name,tag,atts);
        }

        public void endElement ( String uri, String name, String tag ) throws SAXException {
            if (top > 0)
                cache.cacheEndElement(uri,name,tag);
            else next.endElement(uri,name,tag);
        }

        public void characters ( char text[], int start, int length ) throws SAXException {
            if (top > 0)
                cache.cacheCharacters(text,start,length);
            else next.characters(text,start,length);
        }

        public void startPredicate ( int pred ) {
            if (top >= ids.length)
                throw new Error("too many nested predicates");
            positions[top] = cache.byteBuffer.position();
            ids[top] = pred;
            released[top++] = false;
        }

        public void endPredicate ( int pred ) {
            if (top > 0 && ids[top-1] == pred)
                cache.byteBuffer.position(positions[--top]).mark().reset();
        }

        public void releasePredicate ( int pred ) {
            boolean flush = true;
            for (int i = 0; i < top; i++)
                if (ids[i] == pred)
                    released[i] = true;
                else flush &= released[i];
            if (top > 0 && flush) {
                cache.dump(next);
                top = 0;
            }
        }

        public String toString () { return "materialize("+next+")"; }
    }

    /** return the children of the current nodes that have the given tagname */
    final class Child extends XPathHandler {
        final String tagname;       // the tagname of the child
        boolean keep;               // are we keeping or skipping events?
        short level;                // the depth level of the current element

        public Child ( String tagname, XPathHandler next ) {
            super(next);
            this.tagname = tagname;
            keep = false;
            level = 0;
        }

        public void startElement ( String nm, String ln, String qn, Attributes a ) throws SAXException {
            if (level++ == 1)
                keep = tagname.equals("*") || tagname.equals(qn);
            if (keep)
                next.startElement(nm,ln,qn,a);
        }

        public void endElement ( String nm, String ln, String qn ) throws SAXException {
            if (keep)
                next.endElement(nm,ln,qn);
            if (--level == 1)
                keep = false;
        }

        public void characters ( char[] text, int start, int length ) throws SAXException {
            if (keep)
                next.characters(text,start,length);
        }

        public String toString () { return "child("+tagname+","+next+")"; }
    }

    /** return the attribute value of the current nodes that have the given attribute name */
    final class Attribute extends XPathHandler {
        final String attributename;
        short level;                // the depth level of the current element

        public Attribute ( String attribute_name, XPathHandler next ) {
            super(next);
            attributename = attribute_name;
            level = 0;
        }

        public void startElement ( String nm, String ln, String qn, Attributes as ) throws SAXException {
            if (level++ == 0)
                for ( int i = 0; i < as.getLength(); i++ )
                    if (attributename.equals("*") || attributename.equals(as.getQName(i))) {
                        char[] s = as.getValue(i).toCharArray();
                        next.characters(s,0,s.length);
                    }
        }

        public void endElement ( String nm, String ln, String qn ) throws SAXException {
            --level;
        }

        public void characters ( char[] text, int start, int length ) throws SAXException {
        }

        public String toString () { return "attribute("+attributename+","+next+")"; }
    }

    /** Return the descendants of the current nodes that have the given tagname.
     * To handle nested elements with the same tagname, use Descendant
     */
    final class SimpleDescendant extends XPathHandler {
        final String tagname;          // the tagname of the descendant
        boolean keep;                  // are we keeping or skipping events?

        public SimpleDescendant ( String tagname, XPathHandler next ) {
            super(next);
            this.tagname = tagname;
            keep = false;
        }

        public void startElement ( String nm, String ln, String qn, Attributes a ) throws SAXException {
            if (!keep)
                keep = tagname.equals(qn);
            if (keep)
                next.startElement(nm,ln,qn,a);
        }

        public void endElement ( String nm, String ln, String qn ) throws SAXException {
            if (keep) {
                next.endElement(nm,ln,qn);
                keep = !tagname.equals(qn);
            }
        }

        public void characters ( char[] text, int start, int length ) throws SAXException {
            if (keep)
                next.characters(text,start,length);
        }

        public String toString () { return "simple_descendant("+tagname+","+next+")"; }
    }

    /** As efficient as SimpleDescendant when there are no nested elements with the same tagname.
     * It caches only the inner nested subelements with the same tagname.
     */
    final class Descendant extends XPathHandler {
        final static int max_nested_level = 100;
        final String tagname;          // the tagname of the descendant
        int level;                     // # of nested elements with the same tagname
        final Cache[] cache;           // cache[i] caches elements of level i-1

        public Descendant ( String tagname, XPathHandler next ) {
            super(next);
            this.tagname = tagname;
            cache = new Cache[max_nested_level];  // to be created lazily
            level = 0;
        }

        public void startElement ( String nm, String ln, String qn, Attributes a ) throws SAXException {
            if (tagname.equals(qn)) {
                if (level > 0 && cache[level-1] == null)
                    cache[level-1] = new Cache();
                level++;
            };
            for (int i = 1; i < level; i++)
                cache[i-1].cacheStartElement(nm,ln,qn,a);
            if (level > 0)
                next.startElement(nm,ln,qn,a);
        }

        public void endElement ( String nm, String ln, String qn ) throws SAXException {
            if (level > 0)
                next.endElement(nm,ln,qn);
            for (int i = 1; i < level; i++)
                cache[i-1].cacheEndElement(nm,ln,qn);
            if (tagname.equals(qn)) {
                level--;
                if (level == 0 && cache[0] != null)
                    cache[0].dump(next);
                else if (level > 0 && cache[level] != null)
                    cache[level-1].append(cache[level]);
            }
        }

        public void characters ( char[] text, int start, int length ) throws SAXException {
            for (int i = 1; i < level; i++)
                cache[i-1].cacheCharacters(text,start,length);
            if (level > 0)
                next.characters(text,start,length);
        }

        public String toString () { return "descendant("+tagname+","+next+")"; }
    }

    /** propagates all input signals to both next and condition streams but wraps each
     * top-level element in the next stream with start/end Condition signals
     */
    final class Predicate extends XPathHandler {
        int level;
        final XPathHandler condition;   // false, if stream is empty
        final int predicate_id;         // the id of the predicate

        public Predicate ( int predicate_id, XPathHandler condition, XPathHandler next ) {
            super(next);
            this.condition = condition;
            this.predicate_id = predicate_id;
            level = 0;
        }

        public void startPredicate ( int pred ) {
            next.startPredicate(pred);
            condition.startPredicate(pred);
        }

        public void endPredicate ( int pred ) {
            next.endPredicate(pred);
            condition.endPredicate(pred);
        }

        public void releasePredicate ( int pred ) {
            next.releasePredicate(pred);
            condition.releasePredicate(pred);
        }

        public void startDocument () throws SAXException {
            next.startDocument();
            condition.startDocument();
        }

        public void endDocument () throws SAXException {
            next.endDocument();
            condition.endDocument();
        }

        public void startElement ( String nm, String ln, String qn, Attributes a ) throws SAXException {
            if (level++ == 0)
                next.startPredicate(predicate_id);
            next.startElement(nm,ln,qn,a);
            condition.startElement(nm,ln,qn,a);
        }

        public void endElement ( String nm, String ln, String qn ) throws SAXException {
            next.endElement(nm,ln,qn);
            condition.endElement(nm,ln,qn);
            if (--level == 0)
                next.endPredicate(predicate_id);
        }

        public void characters ( char[] text, int start, int length ) throws SAXException {
            next.characters(text,start,length);
            condition.characters(text,start,length);
        }

        public String toString () { return "predicate("+predicate_id+","+condition+","+next+")"; }
    }

    /** generate a releasePredicate signal if the content of the input node is equal to text */
    final class Equals extends XPathHandler {
        final static int max_depth_of_nested_predicates = 100;
        final String value;          // the value to be tested for equality
        final int predicate_id;      // the id of the predicate
        final int[] preds;
        int top;
        boolean suspended;

        public Equals ( int predicate_id, String value, XPathHandler next ) {
            super(next);
            this.value = value;
            this.predicate_id = predicate_id;
            preds = new int[max_depth_of_nested_predicates];
            top = 0;
            suspended = false;
        }

        private boolean compare ( char[] text, int start, int length, String value ) {
            if (length != value.length())
                return false;
            for (int i = 0; i < length; i++)
                if (text[i+start] != value.charAt(i))
                    return false;
            return true;
        }

        public void startPredicate ( int pred ) {
            preds[top++] = pred;
        }

        public void endPredicate ( int pred ) {
            suspended = false;
            for (int i = 0; i < top; i++)
                if (preds[i] == pred) {
                    preds[i] = preds[--top];
                    return;
                }
        }

        public void releasePredicate ( int pred ) {
            if (top == 1 && suspended)
                next.releasePredicate(predicate_id);
            endPredicate(pred);
        }

        public void startDocument () throws SAXException {}

        public void endDocument () throws SAXException {}

        public void startElement ( String nm, String ln, String qn, Attributes a ) throws SAXException {}

        public void endElement ( String nm, String ln, String qn ) throws SAXException {}

        public void characters ( char[] text, int start, int length ) throws SAXException {
            if (compare(text,start,length,value))
                if (top == 0)
                    next.releasePredicate(predicate_id);
                else suspended = true;
        }

        public String toString () { return "equals("+predicate_id+","+value+","+next+")"; }
    }

    /** Converts the SAX data stream to MRData */
    final class MRDataHandler extends XPathHandler {
        Stack<Union> stack = new Stack<Union>();
        Bag children;

        public MRDataHandler () throws Exception {
            super(null);
            stack.clear();
            Tuple t = new Tuple(3);
            children = new Bag();
            t.set(2,children);
            stack.push(new Union((byte)0,t));
        }

        public void start () throws Exception {
            stack.clear();
            Tuple t = new Tuple(3);
            children = new Bag();
            t.set(2,children);
            stack.push(new Union((byte)0,t));
        }

        public Bag value () {
            if (stack.size() != 1)
                return null;
            else return ((Bag)((Tuple)stack.peek().value()).get(2));
        }

        public void startDocument () throws SAXException {}

        public void endDocument () throws SAXException {}

        public void startElement ( String nm, String ln, String qn, Attributes as ) throws SAXException {
            children = new Bag();
            Bag attributes = new Bag();
            for ( int i = 0; i < as.getLength(); i++ )
                attributes.add(new Tuple(new MR_string(as.getQName(i)),new MR_string(as.getValue(i))));
            Tuple t = new Tuple(3);
            t.set(0,new MR_string(qn));
            t.set(1,attributes);
            t.set(2,children);
            stack.push(new Union((byte)0,t));
        }

        public void endElement ( String nm, String ln, String qn ) throws SAXException {
            if (stack.empty())
                throw new SAXException("Ill-formed XML elements: "+qn);
            Union v = stack.pop();
            if (!((MR_string)((Tuple)v.value()).get(0)).get().equals(qn))
                throw new SAXException("Unmatched tags in XML element: "+qn);
            children = (Bag)((Tuple)stack.peek().value()).get(2);
            children.add(v);
        }

        public void characters ( char[] text, int start, int length ) throws SAXException {
            String s = new String(text,start,length);
            if (s.startsWith("{{") && s.endsWith("}}"))
                children.add(new MR_variable(Integer.parseInt(s.substring(2,s.length()-2))));
            else children.add(new Union((byte)1,new MR_string(s)));
        }

        public String toString () { return "MRDataHandler()"; }
    }

    public XPathParser ( Tree xpath ) throws Exception {
        dataConstructor = new MRDataHandler();
        handler = compile_xpath(xpath,new Materialize(dataConstructor),0);
    }

    public XPathHandler compile_xpath ( Tree xpath, XPathHandler next, int cn ) throws Exception {
        if (xpath.is_variable() && xpath.toString().equals("dot"))
            return next;
        if (!xpath.is_node())
            throw new Error("Unrecognized xpath query: "+xpath);
        Node n = (Node) xpath;
        if (n.name().equals("child")) {
            String tag = n.children().head().toString();
            XPathHandler c = (tag.charAt(0) == '@')
                              ? new Attribute(tag.substring(1),next)
                              : new Child(tag,next);
            return compile_xpath(n.children().tail().head(),c,cn);
        } else if (n.name().equals("descendant")) {
            XPathHandler c = new Descendant(n.children().head().toString(),next);
            return compile_xpath(n.children().tail().head(),c,cn);
        } else if (n.name().equals("eq")) {
            Tree value = n.children().tail().head();
            XPathHandler c = new Equals(cn,(value.is_string())
                                            ? ((StringLeaf)value).value()
                                            : value.toString(),
                                        next);
            return compile_xpath(n.children().head(),c,cn);
        } else if (n.name().equals("predicate")) {
            XPathHandler c = new Predicate(cn+1,compile_xpath(n.children().head(),next,cn+1),next);
            return compile_xpath(n.children().tail().head(),c,cn);
        };
        throw new Error("Unrecognized xpath query: "+xpath);
    }
}
