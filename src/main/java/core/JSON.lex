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


%%
%class JSONLex
%public
%line
%char
%cup
%eofval{
  return symbol(jsym.EOF);
%eofval}
%{
  public String text () { return yytext(); }

  public int line_pos () { return yyline+1; }

  public int char_pos () { return yychar; }

  public static Symbol symbol ( int s ) {
    return new Symbol(s);
  }

  public static Symbol symbol ( int s, Object o ) {
    return new Symbol(s,o);
  }

%}

INT = [+-]?[0-9]+
DOUBLE = [+-]?[0-9]+([\.][0-9]+)?([eE][+-]?[0-9]+)?

%%

\"[^\"]*\"	        { return symbol(jsym.STRING,yytext().substring(1,yytext().length()-1)); }
{INT}		        { return symbol(jsym.INTEGER,new Long(yytext())); }
{DOUBLE}         	{ return symbol(jsym.DOUBLE,new Double(yytext())); }
true                    { return symbol(jsym.TRUE); }
false                   { return symbol(jsym.FALSE); }
null                    { return symbol(jsym.NULL); }
\{                      { return symbol(jsym.O_BEGIN); }
\}                      { return symbol(jsym.O_END); }
\[                      { return symbol(jsym.A_BEGIN); }
\]                      { return symbol(jsym.A_END); }
\,                      { return symbol(jsym.COMMA); }
\:                      { return symbol(jsym.COLON); }
[ \t\f]                 { }
[\r\n]                  { }
.                       { throw new Error("Illegal character: "+yytext()); }
