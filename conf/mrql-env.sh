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
#--------------------------------------------------------------------------------
#
# Set Apache MRQL-specific environment variables here.
#
#--------------------------------------------------------------------------------


# Required: The Java installation directory
JAVA_HOME=/root/jdk

# Required: The CUP parser library
# You can download it from http://www2.cs.tum.edu/projects/cup/
CUP_JAR=${HOME}/.m2/repository/net/sf/squirrel-sql/thirdparty/non-maven/java-cup/11a/java-cup-11a.jar

# Required: The JLine library
# You can download from http://jline.sourceforge.net
JLINE_JAR=${HOME}/.m2/repository/jline/jline/1.0/jline-1.0.jar


# Required: Hadoop configuration
HADOOP_VERSION=1.0.3
# The Hadoop installation directory
HADOOP_HOME=${HOME}/hadoop-${HADOOP_VERSION}
# The Hadoop job trackeer (as defined in hdfs-site.xml)
MAPRED_JOB_TRACKER=localhost:9001
# The HDFS namenode URI (as defined in hdfs-site.xml)
FS_DEFAULT_NAME=hdfs://localhost:9000/


# Optional: Hama configuration
HAMA_VERSION=0.6.2
# The Hadoop installation directory
HAMA_HOME=${HOME}/hama-${HAMA_VERSION}
# The Hama configuration as defined in hama-site.xml
BSP_MASTER_ADDRESS=localhost:40000
HAMA_ZOOKEEPER_QUORUM=localhost


# Optional: Spark configuration
SPARK_HOME=${HOME}/spark-0.8.1-incubating-bin-hadoop1
# URI of the Spark master node
SPARK_MASTER=spark://crete:7077
# Spark memory per node
SPARK_MEM="1g"


# Claspaths

HADOOP_JARS=${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/lib/commons-logging-1.1.1.jar:${HADOOP_HOME}/lib/log4j-1.2.15.jar:${HADOOP_HOME}/lib/commons-cli-1.2.jar

HAMA_JAR=${HAMA_HOME}/hama-core-${HAMA_VERSION}.jar

SPARK_JARS=${SPARK_HOME}/assembly/target/scala-2.9.3/*

# for hadoop yarn (build using eg,  mvn -Pyarn -Dhadoop.version=2.2.0 install)
#HADOOP_JARS=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/commons-cli-1.2.jar
