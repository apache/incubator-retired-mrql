#/bin/bash
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
#
# Set MRQL-specific environment variables here.
#
#--------------------------------------------------------------------------------

# Required: The java implementation to use
JAVA_HOME=/root/jdk

# Required: Hadoop configuration
HADOOP_VERSION=1.0.3
HADOOP_HOME=${HOME}/hadoop-${HADOOP_VERSION}
# The Hadoop configuration directory
HADOOP_CONF=${HOME}/conf

# Optional: Hama configuration
HAMA_VERSION=0.5.0
HAMA_HOME=${HOME}/hama-${HAMA_VERSION}
# The Hama configuration directory
HAMA_CONF=${HOME}/conf-hama
HAMA_JAR=${HAMA_HOME}/hama-core-${HAMA_VERSION}.jar

# Optional: Spark configuration
SPARK_HOME=${HOME}/spark-0.7.3
SCALA_LIB=/usr/share/java
SPARK_MASTER=spark://crete:7077
SPARK_DEFAULT_URI=hdfs://localhost:9000/
SPARK_JARS=${SCALA_LIB}/scala-library.jar:${SCALA_LIB}/scala-compiler.jar:${SPARK_HOME}/core/target/scala-2.9.3/classes:${SPARK_HOME}/lib_managed/jars/*:${SPARK_HOME}/lib_managed/bundles/*

# Required: The jflex parser library
# You may install it as a linux package or download it from http://jflex.de/
JFLEX_JAR=/usr/share/java/JFlex.jar

# Required: The CUP parser library
# You may install it as a linux package or download it from http://www2.cs.tum.edu/projects/cup/
CUP_JAR=/usr/share/java/cup.jar

# Hadoop libraries are from a Hadoop tarball
HADOOP_JARS=${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/lib/commons-cli-1.2.jar

# Hadoop libraries are from a YARN tarball
#HADOOP_JARS=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/commons-cli-1.2.jar

# Hadoop libraries are from a Cloudera package
#HADOOP_JARS=/usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hadoop-0.20/lib/commons-cli-1.2.jar

MRQL_CLASSPATH=classes:${HAMA_JAR}:${SPARK_JARS}:${HADOOP_JARS}
