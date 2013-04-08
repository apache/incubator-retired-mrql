#--------------------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#--------------------------------------------------------------------------------
#
# Makefile for MRQL
# Requires: jflex, cup  (these are standard packages in Linux)
#
#--------------------------------------------------------------------------------

# choose the Hadoop installation:
#   tarball  (Hadoop tarball or Cloudera CDH3 tarball -- preferred)
#   package  (Cloudera CDH3 package-based 0.20.2)
INSTALLATION=tarball

# the jar of the CUP parser (you may download it from http://www2.cs.tum.edu/projects/cup/ )
CUPJAR=/usr/share/java/cup.jar

# optional: the Hama jar for BSP processing
HAMAJAR=${HOME}/hama-0.5.0/hama-core-0.5.0.jar

MRQL_CLASSPATH=classes:lib/gen.jar:lib/jline-1.0.jar:${CUPJAR}

ifeq (${INSTALLATION},tarball)
  VERSION=1.0.3
  HADOOP=${HOME}/hadoop-${VERSION}
  CLASSPATH=${MRQL_CLASSPATH}:${HADOOP}/hadoop-core-${VERSION}.jar:${HADOOP}/lib/commons-cli-1.2.jar:${HAMAJAR}
else
  HADOOP=/usr/lib/hadoop-0.20
  CLASSPATH=${MRQL_CLASSPATH}:/usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hadoop-0.20/lib/commons-cli-1.2.jar:${HAMAJAR}
endif

export CLASSPATH

JAVAC = javac -g:none -d classes
JAVA = java
JAR = jar
JFLEX = jflex --quiet --nobak
CUP = cup -nosummary
GEN = ${JAVA} Gen.Main

sources := src/*.java
mr_sources := ${sources} src/MapReduce/*.java
bsp_sources := ${sources} src/BSP/*.java


all: common
	@${GEN} src/MapReduce/*.gen -o tmp
	@${JAVAC} ${mr_sources} tmp/*.java
	@${JAR} cf lib/mrql.jar -C classes/ .

bsp: common
	@${GEN} src/BSP/*.gen -o tmp
	@${JAVAC} ${bsp_sources} tmp/*.java
	@${JAR} cf lib/mrql-bsp.jar -C classes/ .

common: clean_build mrql_parser json_parser
	@cd classes; ${JAR} xf ../lib/gen.jar; ${JAR} xf ${CUPJAR}; ${JAR} xf ../lib/jline-1.0.jar; cd ..
	@${GEN} src/*.gen -o tmp

clean_build:
	@rm -rf classes tmp
	@mkdir -p classes tmp

mrql_parser:
	@${JFLEX} src/mrql.lex -d tmp
	@${GEN} src/mrql.cgen -o tmp/mrql.cup
	@${CUP} -parser MRQLParser tmp/mrql.cup
	@mv sym.java MRQLParser.java tmp/

json_parser:
	@${JFLEX} src/JSON.lex -d tmp
	@${CUP} -parser JSONParser -symbols jsym src/JSON.cup
	@mv jsym.java JSONParser.java tmp/

tgz: clean
	@cd ..; tar cfz q.tgz mrql/lib mrql/Makefile mrql/NOTICE mrql/LICENCE mrql/README mrql/src \
			mrql/queries mrql/bin mrql/conf mrql/conf-hama; cd mrql

tgz-all: clean
	@rm -rf lib/mrql.jar lib/mrql-bsp.jar
	@cd ..; tar cfz q.tgz mrql; cd mrql

clean: 
	@/bin/rm -rf *~ */*~ */*/*~ classes mrql tmp
