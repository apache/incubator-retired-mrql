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

import java_cup.runtime.Symbol;
import org.apache.mrql.gen.Tree;

%%
%class MRQLLex
%public
%line
%char
%ignorecase
%cup
%state COMMENT
%state TEMPLATE
%eofval{
  return symbol(sym.EOF);
%eofval}
%{

  static int prev_char_pos = -1;

  public static int[] nest = new int[1000];
  public static int nest_pos = 0;

  static String template = null;

  public static void reset () {
    nest_pos = 0;
    nest[0] = 0;
    prev_char_pos = -1;
  }

  public static void record_begin () {
    nest[++nest_pos] = 0;
  }

  public static void record_end () {
    nest_pos--;
  }

  public int line_pos () { return yyline+1; }

  public int char_pos () { return yychar-prev_char_pos; }

  public Symbol symbol ( int s ) {
    Tree.line_number = line_pos();
    Tree.position_number = char_pos();
    return new Symbol(s);
  }

  public Symbol symbol ( int s, Object o ) {
    Tree.line_number = line_pos();
    Tree.position_number = char_pos();
    return new Symbol(s,o);
  }

  public void error ( String msg ) {
    System.err.println("*** Scanner Error: " + msg + " (line: " + line_pos() + ", position: " + char_pos() + ")");
    if (Config.testing)
      throw new Error("Scanner Error");
  }

  public String format ( String s ) {
    return s.replaceAll("\\\\t", "\t").replaceAll("\\\\n", "\n");
  }
%}

ID=[a-zA-Z][a-zA-Z0-9_]*
INT = [0-9]+
DOUBLE = [0-9]+([\.][0-9]+)?([eE][+-]?[0-9]+)?

%%

<YYINITIAL> {INT}		{ return symbol(sym.Integer,new Long(yytext())); }
<YYINITIAL> {DOUBLE}	        { return symbol(sym.Double,new Double(yytext())); }
<YYINITIAL> "["{ID}"|"          { yybegin(TEMPLATE); template = ""; return symbol(sym.START_TEMPLATE,yytext()); }
<YYINITIAL> "if"		{ return symbol(sym.IF); }
<YYINITIAL> "then"		{ return symbol(sym.THEN); }
<YYINITIAL> "else"		{ return symbol(sym.ELSE); }
<YYINITIAL> "select"		{ return symbol(sym.SELECT); }
<YYINITIAL> "from"		{ return symbol(sym.FROM); }
<YYINITIAL> "having"		{ return symbol(sym.HAVING); }
<YYINITIAL> "["			{ nest[nest_pos]++; return symbol(sym.LB); }
<YYINITIAL> "]"			{ nest[nest_pos]--; return symbol(sym.RB); }
<YYINITIAL> "("			{ nest[nest_pos]++; return symbol(sym.LP); }
<YYINITIAL> ")"			{ nest[nest_pos]--; return symbol(sym.RP); }
<YYINITIAL> "{"			{ nest[nest_pos]++; return symbol(sym.LSB); }
<YYINITIAL> "}}"		{ nest[nest_pos]--; if (nest_pos > 0 && nest[nest_pos] == 0) { record_end(); yybegin(TEMPLATE); } else return symbol(sym.RSB); }
<YYINITIAL> "}"			{ nest[nest_pos]--; return symbol(sym.RSB); }
<YYINITIAL> "+"			{ return symbol(sym.PLUS); }
<YYINITIAL> "-"			{ return symbol(sym.MINUS); }
<YYINITIAL> "*"			{ return symbol(sym.TIMES); }
<YYINITIAL> "/"		        { return symbol(sym.DIV); }
<YYINITIAL> "mod"		{ return symbol(sym.MOD); }
<YYINITIAL> "%"		        { return symbol(sym.MOD); }
<YYINITIAL> "="			{ return symbol(sym.EQ); }
<YYINITIAL> "<>"		{ return symbol(sym.NEQ); }
<YYINITIAL> "<="		{ return symbol(sym.LEQ); }
<YYINITIAL> "<"			{ return symbol(sym.LT); }
<YYINITIAL> ">="		{ return symbol(sym.GEQ); }
<YYINITIAL> "!"		        { return symbol(sym.EXCLAMATION); }
<YYINITIAL> ">"			{ return (nest_pos > 0 && nest[nest_pos] == 0) ? symbol(sym.GTR) : symbol(sym.GT); }
<YYINITIAL> \\  		{ return symbol(sym.BSLASH); }
<YYINITIAL> "and"		{ return symbol(sym.AND); }
<YYINITIAL> "or"		{ return symbol(sym.OR); }
<YYINITIAL> "not"		{ return symbol(sym.NOT); }
<YYINITIAL> "union"		{ return symbol(sym.UNION); }
<YYINITIAL> "intersect"		{ return symbol(sym.INTERSECT); }
<YYINITIAL> "except"		{ return symbol(sym.EXCEPT); }
<YYINITIAL> "exists"		{ return symbol(sym.EXISTS); }
<YYINITIAL> "in"		{ return symbol(sym.IN); }
<YYINITIAL> "let"		{ return symbol(sym.LET); }
<YYINITIAL> ","			{ return symbol(sym.COMMA); }
<YYINITIAL> "."			{ return symbol(sym.DOT); }
<YYINITIAL> ":="		{ return symbol(sym.ASSIGN); }
<YYINITIAL> ":"			{ return symbol(sym.COLON); }
<YYINITIAL> ";"	        	{ return symbol(sym.SEMI); }
<YYINITIAL> "#"	        	{ return symbol(sym.SHARP); }
<YYINITIAL> "@"	        	{ return symbol(sym.ATSYM); }
<YYINITIAL> \|          	{ return symbol(sym.SEP); }
<YYINITIAL> "where"		{ return symbol(sym.WHERE); }
<YYINITIAL> "order"		{ return symbol(sym.ORDER); }
<YYINITIAL> "group"		{ return symbol(sym.GROUP); }
<YYINITIAL> "by"		{ return symbol(sym.BY); }
<YYINITIAL> "asc"		{ return symbol(sym.ASCENDING); }
<YYINITIAL> "desc"	        { return symbol(sym.DESCENDING); }
<YYINITIAL> "function"		{ return symbol(sym.FUNCTION); }
<YYINITIAL> "macro"		{ return symbol(sym.MACRO); }
<YYINITIAL> "distinct"		{ return symbol(sym.DISTINCT); }
<YYINITIAL> "as"		{ return symbol(sym.AS); }
<YYINITIAL> "some"		{ return symbol(sym.SOME); }
<YYINITIAL> "all"		{ return symbol(sym.ALL); }
<YYINITIAL> "store"		{ return symbol(sym.STORE); }
<YYINITIAL> "dump"		{ return symbol(sym.DUMP); }
<YYINITIAL> "type"		{ return symbol(sym.TYPE); }
<YYINITIAL> "data"		{ return symbol(sym.DATA); }
<YYINITIAL> "case"		{ return symbol(sym.CASE); }
<YYINITIAL> "xpath"		{ return symbol(sym.XPATH); }
<YYINITIAL> "repeat"		{ return symbol(sym.REPEAT); }
<YYINITIAL> "step"		{ return symbol(sym.STEP); }
<YYINITIAL> "limit"		{ return symbol(sym.LIMIT); }
<YYINITIAL> "import"		{ return symbol(sym.IMPORT); }
<YYINITIAL> "parser"		{ return symbol(sym.PARSER); }
<YYINITIAL> "include"		{ return symbol(sym.INCLUDE); }
<YYINITIAL> "aggregation"	{ return symbol(sym.AGGREGATION); }

<YYINITIAL> {ID}		{ return symbol(sym.Variable,yytext()); }

<YYINITIAL> "/*"	        { yybegin(COMMENT); }
<COMMENT> "*/"                  { yybegin(YYINITIAL); }
<COMMENT> [ \t\f]               { }
<COMMENT> [\r\n]                { prev_char_pos = yychar; }
<COMMENT> .		        { }

<TEMPLATE> "|]"                 { yybegin(YYINITIAL); String s = template; template = ""; return symbol(sym.END_TEMPLATE,s); }
<TEMPLATE> "{{"                 { yybegin(YYINITIAL); record_begin(); nest[nest_pos]++; String s = template; template = ""; return symbol(sym.TEXT,s); }
<TEMPLATE> [ \t\f]              { template += yytext(); }
<TEMPLATE> [\r\n]               { template += yytext(); prev_char_pos = yychar; }
<TEMPLATE> .                    { template += yytext(); }

<YYINITIAL> "//"[^\n]*\n        { prev_char_pos = yychar; }

<YYINITIAL> \"[^\"]*\"	        { return symbol(sym.String,format(yytext().substring(1,yytext().length()-1))); }
<YYINITIAL> \'[^\']*\'	        { return symbol(sym.String,format(yytext().substring(1,yytext().length()-1))); }

<YYINITIAL> [ \t\f]             { }
<YYINITIAL> [\r\n]              { prev_char_pos = yychar; }

<YYINITIAL> .                   { error("Illegal character: "+yytext()); }
