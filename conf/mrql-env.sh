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
# To rebuild Apache MRQL from sources:
#
# build MRQL on Hadoop 1.x:
# mvn -Dhadoop.version=1.0.3 install
#
# build MRQL on Hadoop 2.x (yarn):
# mvn -Pyarn -Dyarn.version=2.2.0 -Dhadoop.version=1.2.1 install
#
# build MRQL on Hadoop 0.20.x:
# mvn -PMultipleInputs -Dhadoop.version=0.20.2 install
#
#--------------------------------------------------------------------------------
#
# Set Apache MRQL-specific environment variables here:


# Required: The Java installation directory
JAVA_HOME=/root/jdk

# Required: The CUP parser library
# You can download it from http://www2.cs.tum.edu/projects/cup/
CUP_JAR=${HOME}/.m2/repository/net/sf/squirrel-sql/thirdparty/non-maven/java-cup/11a/java-cup-11a.jar

# Required: The JLine library
# You can download it from http://jline.sourceforge.net
JLINE_JAR=${HOME}/.m2/repository/jline/jline/1.0/jline-1.0.jar


# Required: Hadoop configuration. Supports versions 0.20.x, 1.x, and 2.x (YARN)
HADOOP_VERSION=2.2.0
# The Hadoop installation directory
HADOOP_HOME=${HOME}/hadoop-${HADOOP_VERSION}
# The Hadoop job tracker (as defined in mapred-site.xml)
MAPRED_JOB_TRACKER=localhost:9001
# The HDFS namenode URI (as defined in core-site.xml)
FS_DEFAULT_NAME=hdfs://localhost:9000/


# Optional: Hama configuration. Supports versions 0.6.2 and 0.6.3 (but not 0.6.4)
HAMA_VERSION=0.6.3
# The Hadoop installation directory
HAMA_HOME=${HOME}/hama-${HAMA_VERSION}
# The Hama configuration (as defined in hama-site.xml)
BSP_MASTER_ADDRESS=localhost:40000
HAMA_ZOOKEEPER_QUORUM=localhost


# Optional: Spark configuration. Supports version 1.0.0 only
# (Spark versions 0.8.1, 0.9.0, and 0.9.1 are supported by MRQL 0.9.0)
# Use either the Spark prebuilts bin-hadoop1 or bin-hadoop2 (Yarn)
# Tested in local, standalone deploy, and Yarn modes
SPARK_HOME=${HOME}/spark-1.0.0-bin-hadoop2
# URI of the Spark master node (to run Spark on a YARN cluster, set it to "yarn-client")
SPARK_MASTER=spark://crete:7077
# For a Yarn cluster set it to the number of workers to start on,
#  for local/standalone set it to 1
SPARK_WORKER_INSTANCES=1
# Number of cores for the workers
SPARK_WORKER_CORES=2
# Memory per Worker (e.g. 1000M, 2G)
SPARK_WORKER_MEMORY=2G
# Memory for Master (e.g. 1000M, 2G)
SPARK_MASTER_MEMORY=512M

# Claspaths

HAMA_JAR=${HAMA_HOME}/hama-core-${HAMA_VERSION}.jar

# YARN-enabled assembly jar
if [[ -d ${SPARK_HOME}/assembly/target ]]; then
   # old Spark (0.x)
   SPARK_JAR=`ls ${SPARK_HOME}/assembly/target/scala-*/*.jar`
else if [[ -d ${SPARK_HOME}/lib ]]; then
   # new Spark (1.x)
   SPARK_JAR=`ls ${SPARK_HOME}/lib/spark-assembly-*.jar`
fi
fi

if [[ -f ${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar ]]; then
   # hadoop 2.x (YARN)
   HADOOP_JARS=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/lib/log4j-1.2.17.jar:${HADOOP_HOME}/share/hadoop/common/lib/commons-cli-1.2.jar
else
   # hadoop 1.x or 0.20.x
   HADOOP_JARS=${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/lib/commons-logging-1.1.1.jar:${HADOOP_HOME}/lib/log4j-1.2.15.jar:${HADOOP_HOME}/lib/commons-cli-1.2.jar
fi
