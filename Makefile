#--------------------------------------------------------------------------------
#
#  Makefile for MRQL
#  Programmer: Leonidas Fegaras
#  Date: 08/01/12
#
#--------------------------------------------------------------------------------

# choose the Hadoop installation:
#   tarball  (Cloudera CDH3 or Hadoop 0.20.* tarball -- preferred)
#   package  (Cloudera CDH3 package-based 0.20.2)
#   hadoop   (Apache Hadoop tarball 0.21.0 -- not for Amazon Elastic MapReduce)
INSTALLATION=tarball

# optional: the Hama jar for BSP processing
HAMAJAR=${HOME}/hama-0.5.0/hama-core-0.5.0.jar

# yes, if you are using a Hadoop's 0.20.* distribution that doesn't provide the MultipleInput class
#MultipleInputs=yes

ifeq (${INSTALLATION},tarball)
  VERSION=1.0.3
  #VERSION=0.20.2-CDH3B4
  #VERSION=0.20.205.0
  HADOOP=${HOME}/hadoop-${VERSION}
  CLASSPATH=classes:gen.jar:jline-1.0.jar:${HADOOP}/hadoop-core-${VERSION}.jar:${HADOOP}/lib/commons-cli-1.2.jar:${HAMAJAR}
else
ifeq (${INSTALLATION},package)
  HADOOP=/usr/lib/hadoop-0.20
  CLASSPATH=classes:gen.jar:jline-1.0.jar:/usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hadoop-0.20/lib/commons-cli-1.2.jar:${HAMAJAR}
else
  VERSION=0.21.0
  HADOOP=${HOME}/hadoop-${VERSION}
  CLASSPATH=classes:gen.jar:jline-1.0.jar:${HADOOP}/hadoop-common-${VERSION}.jar:${HADOOP}/hadoop-mapred-${VERSION}.jar:${HADOOP}/hadoop-hdfs-${VERSION}.jar:${HADOOP}/lib/commons-cli-1.2.jar:${HAMAJAR}
endif
endif


export CLASSPATH
export HADOOP_HOME=${HADOOP}

JAVAC=javac -d classes -O
JAVA=java
JAR=jar
MSOURCES=tmp/MRQLParser.java tmp/MRQLLex.java tmp/Translate.out.java tmp/Interpreter.out.java \
	tmp/QueryPlan.out.java tmp/sym.java src/Main.java src/SystemFunctions.java src/Function.java \
	src/MapReduceAlgebra.java src/MapReduceData.java src/DataSource.java src/MRData.java \
	src/Bag.java src/Tuple.java src/Union.java src/Lambda.java src/MR_bool.java src/MR_byte.java \
	src/MR_short.java src/MR_int.java src/MR_long.java src/MR_float.java src/MR_double.java \
	src/MR_char.java src/MR_string.java src/MR_dataset.java \
	src/Plan.java src/XMLDataSource.java tmp/Compiler.out.java tmp/BSPTranslate.out.java \
	tmp/JSONLex.java tmp/JSONParser.java tmp/jsym.java src/JSONDataSource.java
BSOURCES=${MSOURCES} src/BSPInputFormat.java tmp/BSPEvaluator.out.java src/Hama.java
MRSOURCES=${MSOURCES} src/MapReducePlan.java src/MapReduceInputFormat.java tmp/MapReduceEvaluator.out.java
ifeq (${MultipleInputs},yes)
SOURCES=${MRSOURCES} src/MultipleInputs/MultipleInputs.java src/MultipleInputs/DelegatingInputFormat.java \
	src/MultipleInputs/DelegatingMapper.java src/MultipleInputs/TaggedInputSplit.java src/MultipleInputs/DelegatingRecordReader.java
else
SOURCES=${MRSOURCES}
endif


all:	${SOURCES}
	@rm -rf classes/hadoop; cd classes; ${JAR} xf ../gen.jar; ${JAR} xf ../jline-1.0.jar; cd ..
	@${JAVAC} ${SOURCES}
	@${JAR} cf mrql.jar -C classes/ .

bsp:	${BSOURCES}
	@rm -rf classes/hadoop; cd classes; ${JAR} xf ../gen.jar; ${JAR} xf ../jline-1.0.jar; cd ..
	@${JAVAC} ${BSOURCES}
	@${JAR} cf mrql-bsp.jar -C classes/ .

tmp/%.out.java: src/%.gen
		@${JAVA} Gen.Main $< tmp/$*.out.java

tmp/MRQLLex.java: src/mrql.lex
		  @${JAVA} JLex.Main src/mrql.lex
		  @mv src/mrql.lex.java tmp/MRQLLex.java

tmp/MRQLParser.java: src/mrql.gen
		     @${JAVA} Gen.Main src/mrql.gen tmp/mrql.cup
		     @${JAVA} java_cup.Main -parser MRQLParser tmp/mrql.cup
		     @mv sym.java MRQLParser.java tmp/

tmp/JSONLex.java: src/JSON.lex
		  @${JAVA} JLex.Main src/JSON.lex
		  @mv src/JSON.lex.java tmp/JSONLex.java

tmp/JSONParser.java: src/JSON.cup
		     @${JAVA} java_cup.Main -parser JSONParser -symbols jsym src/JSON.cup
		     @mv jsym.java JSONParser.java tmp/

tgz:	clean
	cd ..; tar cfz q.tgz mrql/mrql.jar mrql/mrql-bsp.jar mrql/gen.jar mrql/jline-1.0.jar mrql/Makefile \
	mrql/COPYRIGHT mrql/src mrql/queries mrql/doc mrql/classes mrql/bin mrql/tmp mrql/conf \
	mrql/conf-hama mrql/COPYRIGHT; cd mrql

tgz-all: clean
	 @/bin/rm -rf mrql.jar my_mrql.jar mrql-bsp.jar
	 @cd ..; tar cfz q.tgz mrql; cd mrql

clean: 
	@/bin/rm -rf *~ */*~ classes/* mrql/* tmp/* tmp/.*.crc benchmarks.jar
