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

import java_cup.runtime.Symbol;

final class Context {
    public int[] parentheses;
    public boolean[] match_begin;
    public int current;
    public Context () {
	current = 0;
	parentheses = new int[1000];
	match_begin = new boolean[1000];
	parentheses[0] = 0;
	match_begin[0] = false;
    }
    public boolean no_parentheses () {
	return parentheses[current] == 0;
    }
    public void new_context () {
	parentheses[++current] = 0;
	match_begin[current] = false;
    }
    public void close_context ( GenLex lex ) {
	if (parentheses[current--] > 0)
	    lex.error("Unbalanced Parentheses in Gen construction/pattern");
	if (current < 0)
	    lex.error("Wrong match statement");
    }
    public void open_parenthesis () {
	parentheses[current]++;
    }
    public boolean close_parenthesis () {
	return (--parentheses[current] == 0) && current > 0 && !match_begin[current];
    }
    public void in_match_body () {
	match_begin[current] = true;
    }
    public boolean match_body () { return match_begin[current]; }
}
%%
%state gen
%class GenLex
%public
%line
%char
%cup

%eofval{ 
  return new java_cup.runtime.Symbol(GenSym.EOF);
%eofval}

%{
  public void error ( String msg ) {
     throw new Error("*** Lexical Error: " + msg + " (line: " + line_pos() + ", position: " + char_pos() + ")");
  }

  public static Context context = new Context();

  static int prev_char_pos = -1;

  public int line_pos () { return yyline+1; }

  public int char_pos () { return yychar-prev_char_pos; }

  public Symbol symbol ( int s ) {
      Tree.line_number = line_pos();
      Tree.position_number = char_pos();
      //System.out.println(context.parentheses[context.current]+" "+context.match_begin[context.current]+" "+GenParser.print(new Symbol(s)));
      return new Symbol(s);
  }

  public Symbol symbol ( int s, Object o ) {
      Tree.line_number = line_pos();
      Tree.position_number = char_pos();
      //System.out.println(context.parentheses[context.current]+" "+context.match_begin[context.current]+" "+GenParser.print(new Symbol(s,o)));
      return new Symbol(s,o);
  }
%}

DIGIT   = [0-9]
ID      = [a-zA-Z_][a-zA-Z0-9_]*
OPER    = [!@#$%\^\&*-+=|\\~]+
NEWLINE = [\n\r]
DIGITS = {DIGIT}+
INT = ({DIGIT}|[1-9]{DIGITS}|-{DIGIT}|-[1-9]{DIGITS})
FRAC = [.]{DIGITS}
EXP = [eE][+-]?{DIGITS}
DOUBLE = ({INT}{FRAC}|{INT}{EXP}|{INT}{FRAC}{EXP})

%%
<gen> {INT}			{ return symbol(GenSym.LONG,new Long(yytext())); }
<gen> {DOUBLE}          	{ return symbol(GenSym.DOUBLE,new Double(yytext())); }
<gen> ":"			{ yybegin(YYINITIAL);
				  context.close_context(this);
				  return symbol(GenSym.COLON); }
<gen> "_"			{ return symbol(GenSym.ANY); }
<gen> ","			{ return symbol(GenSym.COMMA); }
<gen> "`("			{ context.new_context();
				  context.open_parenthesis();
				  yybegin(YYINITIAL);
				  return symbol(GenSym.BQP);
				}
<gen> "...("			{ context.new_context();
				  context.open_parenthesis();
				  yybegin(YYINITIAL);
				  return symbol(GenSym.DOTSP);
				}
<gen> "`"			{ return symbol(GenSym.BQ); }
<gen> "..."			{ return symbol(GenSym.DOTS); }
<gen> "("			{ context.open_parenthesis();
				  return symbol(GenSym.LP);
				}
<gen> ")"			{ context.close_parenthesis();
				  return symbol(GenSym.RP);
				}
<gen> "["			{ context.open_parenthesis();
				  return symbol(GenSym.LB);
				}
<gen> "]"			{ context.close_parenthesis();
				  if (context.no_parentheses())
				  {  yybegin(YYINITIAL);
				     context.close_context(this);
				  };
				  return symbol(GenSym.RB);
				}
<gen> ">"			{ yybegin(YYINITIAL);
				  context.close_parenthesis();
				  context.close_context(this);
				  return symbol(GenSym.GT);
				}
<gen> "is"			{ return symbol(GenSym.IS); }
<gen> {ID}			{ return symbol(GenSym.ID,yytext()); }
<gen> {OPER}			{ return symbol(GenSym.OPER,yytext()); }
<gen> \/\*[^*/]*\*\/		{ for (char c: yytext().toCharArray())
      				      if (c=='\n' || c=='\r')
				          GenParser.newlines++;
				   prev_char_pos = yychar; }
<gen> "//"[^\n\r]*		{ prev_char_pos = 0; }
<gen> [ \t\f]			{}
<gen> {NEWLINE}		        { GenParser.newlines++; prev_char_pos = yychar; }
<gen> .				{ error("Illegal character in Gen construction/pattern: "+yytext()); }
<YYINITIAL> "match"		{ return symbol(GenSym.MATCH); }
<YYINITIAL> "case"		{ if (!context.match_body())
				     return symbol(GenSym.CHAR,yytext());
				  context.new_context();
				  yybegin(gen);
				  return symbol(GenSym.CASE);
				}
<YYINITIAL> "fail"		{ return symbol(GenSym.FAIL); }
<YYINITIAL> "`"			{ error("Backquote outside a Gen construction/pattern"); }
<YYINITIAL> "#<"		{ context.new_context();
				  context.open_parenthesis();
				  yybegin(gen);
				  return symbol(GenSym.META);
				}
<YYINITIAL> "#["		{ context.new_context();
                                  context.open_parenthesis();
				  yybegin(gen);
				  return symbol(GenSym.METAL);
				}
<YYINITIAL> "{"		        { context.open_parenthesis();
	    			  if (context.match_body())
				      return symbol(GenSym.LSB);
				  else return symbol(GenSym.CHAR,yytext());
				}
<YYINITIAL> "("|"["		{ context.open_parenthesis();
				  return symbol(GenSym.CHAR,yytext());
				}
<YYINITIAL> "}"			{ context.close_parenthesis();
	    			  if (context.match_body())
				      return symbol(GenSym.RSB);
				  else return symbol(GenSym.CHAR,yytext());
				}
<YYINITIAL> ")"			{ if (context.close_parenthesis())
				  {  context.close_context(this);
				     yybegin(gen);
				     return symbol(GenSym.RP);
				  } else return symbol(GenSym.CHAR,yytext());
				}
<YYINITIAL> "]"			{ if (context.close_parenthesis())
				  {  context.close_context(this);
				     yybegin(gen);
				     return symbol(GenSym.RB);
				  } else return symbol(GenSym.CHAR,yytext());
				}
\"[^\"]*\"			{ return symbol(GenSym.CSTRING,yytext().substring(1,yytext().length()-1)); }
<YYINITIAL> {ID}         	{ return symbol(GenSym.CHAR,yytext()); }
<YYINITIAL> {OPER}         	{ return symbol(GenSym.CHAR,yytext()); }
<YYINITIAL> \/\*[^*/]*\*\/	{ prev_char_pos = yychar; return symbol(GenSym.CHAR,yytext()); }
<YYINITIAL> "//"[^\n\r]*        { prev_char_pos = 0; return symbol(GenSym.CHAR,yytext()); }
<YYINITIAL> [ \t\f]             { return symbol(GenSym.CHAR,yytext()); }
<YYINITIAL> {NEWLINE}           { prev_char_pos = yychar; return symbol(GenSym.CHAR,yytext()); }
<YYINITIAL> .                   { return symbol(GenSym.CHAR,yytext()); }
