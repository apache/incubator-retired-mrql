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
#
#--------------------------------------------------------------------------------

MRQL_HOME=$(shell readlink -f .)

include conf/mrql-env.sh
include conf/mrql-ant-env.sh

export CLASSPATH=${MRQL_CLASSPATH}:${JFLEX_JAR}:${CUP_JAR}:${JLINE_JAR}
export JAVA_HOME

JAVAC = ${JAVA_HOME}/bin/javac -g:none -d classes
JAVA = ${JAVA_HOME}/bin/java
JAR = ${JAVA_HOME}/bin/jar
JFLEX = ${JAVA} -jar ${JFLEX_JAR} --quiet --nobak
CUP = ${JAVA} -jar ${CUP_JAR} -nosummary
GEN = ${JAVA} org.apache.mrql.gen.Main

sources := src/main/java/core/*.java
mr_sources := ${sources} src/main/java/MapReduce/*.java
bsp_sources := ${sources} src/main/java/BSP/*.java
spark_sources := ${sources} src/main/java/spark/*.java


all: common
	@${GEN} src/main/java/MapReduce/*.gen -o tmp
	@${JAVAC} ${mr_sources} tmp/*.java
	@${JAR} cf lib/mrql-mr-${MRQL_VERSION}.jar -C classes/ .

bsp: common
	@${GEN} src/main/java/BSP/*.gen -o tmp
	@${JAVAC} ${bsp_sources} tmp/*.java
	@${JAR} cf lib/mrql-bsp-${MRQL_VERSION}.jar -C classes/ .

spark: common
	@${GEN} src/main/java/spark/*.gen -o tmp
	@${JAVAC} ${spark_sources} tmp/*.java
	@${JAR} cf lib/mrql-spark-${MRQL_VERSION}.jar -C classes/ .

common: clean_build gen mrql_parser json_parser
	@cd classes; ${JAR} xf ${CUP_JAR}; ${JAR} xf ${JLINE_JAR}; cd ..
	@${GEN} src/main/java/core/*.gen -o tmp

clean_build:
	@rm -rf classes tmp
	@mkdir -p classes tmp tests/results tests/results/mr-memory tests/results/bsp-memory tests/results/hadoop tests/results/bsp tests/results/spark

gen:
	@${JFLEX} src/main/java/gen/gen.lex -d tmp
	@${CUP} -symbols GenSym -parser GenParser src/main/java/gen/gen.cup
	@mv GenParser.java GenSym.java tmp/
	@${JAVAC} src/main/java/gen/*java tmp/GenLex.java tmp/GenParser.java tmp/GenSym.java

mrql_parser:
	@${JFLEX} src/main/java/core/mrql.lex -d tmp
	@${GEN} src/main/java/core/mrql.cgen -o tmp/mrql.cup
	@${CUP} -parser MRQLParser tmp/mrql.cup
	@mv sym.java MRQLParser.java tmp/

json_parser:
	@${JFLEX} src/main/java/core/JSON.lex -d tmp
	@${CUP} -parser JSONParser -symbols jsym src/main/java/core/JSON.cup
	@mv jsym.java JSONParser.java tmp/

validate: validate_hadoop validate_hama validate_spark

validate_hadoop:
	@echo "Evaluating test queries in memory (Map-Reduce mode):"
	@${JAVA} -classpath ${MRQL_HOME}/lib/mrql-mr-${MRQL_VERSION}.jar:${MRQL_CLASSPATH} org.apache.mrql.Test tests/queries tests/results/mr-memory tests/error_log.txt
	@echo "Evaluating test queries in Hadoop local mode:"
	@${JAVA} -classpath ${MRQL_HOME}/lib/mrql-mr-${MRQL_VERSION}.jar:${MRQL_CLASSPATH} org.apache.mrql.Test -local tests/queries tests/results/hadoop tests/error_log.txt 2>/dev/null

validate_hama:
	@echo "Evaluating test queries in memory (BSP mode):"
	@${JAVA} -classpath ${MRQL_HOME}/lib/mrql-bsp-${MRQL_VERSION}.jar:${MRQL_CLASSPATH} org.apache.mrql.Test tests/queries tests/results/bsp-memory tests/error_log.txt
	@echo "Evaluating test queries in Hama local mode:"
	@${JAVA} -classpath ${MRQL_HOME}/lib/mrql-bsp-${MRQL_VERSION}.jar:${MRQL_CLASSPATH} org.apache.mrql.Test -local tests/queries tests/results/bsp tests/error_log.txt 2>/dev/null

validate_spark:
	@echo "Evaluating test queries in Spark local mode:"
	@${JAVA} -classpath ${MRQL_HOME}/lib/mrql-spark-${MRQL_VERSION}.jar:${MRQL_CLASSPATH} org.apache.mrql.Test -local tests/queries tests/results/spark tests/error_log.txt 2>/dev/null

javadoc: all
	@${JAVA_HOME}/bin/javadoc ${mr_sources} tmp/*.java -d /tmp/web-mrql

clean_tests:
	@/bin/rm -rf tests/results/*/*

clean: 
	@/bin/rm -rf *~ */*~ */*/*~ classes mrql-tmp tmp null tests/error_log.txt */dependency-reduced-pom.xml
