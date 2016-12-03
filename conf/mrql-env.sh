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
# build MRQL on Hadoop 2.x (yarn):
# mvn -Dyarn.version=2.2.0 clean install
#
# build MRQL on Hadoop 1.x:
# mvn -Dhadoop1 -Dhadoop.version=1.0.3 clean install
#
#--------------------------------------------------------------------------------
#
# Set Apache MRQL-specific environment variables here:


# Required: The Java installation directory
if [ -z ${JAVA_HOME} ]; then
   export JAVA_HOME=/usr/lib/jvm/java-8-oracle
fi
if [ ! -e ${JAVA_HOME} ]; then
    echo "*** Non-existent JAVA_HOME"
    exit -1
fi

# Required: The CUP parser library
# You can download it from http://www2.cs.tum.edu/projects/cup/
CUP_JAR=${HOME}/.m2/repository/net/sf/squirrel-sql/thirdparty/non-maven/java-cup/11a/java-cup-11a.jar

# Required: The JLine library
# You can download it from http://jline.sourceforge.net
JLINE_JAR=${HOME}/.m2/repository/jline/jline/1.0/jline-1.0.jar


# Hadoop configuration. Supports versions 1.x and 2.x (YARN)
# The Hadoop installation directory
if [ -z ${HADOOP_HOME} ]; then
    HADOOP_VERSION=2.7.1
    HADOOP_HOME=${HOME}/hadoop-${HADOOP_VERSION}
fi
# The Hadoop configuration directory. Set it to empty to use the default
HADOOP_CONFIG=${HADOOP_HOME}/etc/hadoop
# The Hadoop job tracker (eg, localhost:9001). If empty, it is the one defined in mapred-site.xml
MAPRED_JOB_TRACKER=
# The HDFS namenode URI (eg, hdfs://localhost:9000/). If empty, it is the one defined in core-site.xml
FS_DEFAULT_NAME=

# Optional: Hama configuration. Supports versions 0.6.2, 0.6.3, 0.6.4, and 0.7.0, and 0.7.1
HAMA_VERSION=0.7.1
# The Hama installation directory
HAMA_HOME=${HOME}/hama-${HAMA_VERSION}
# The Hama configuration directory
HAMA_CONFIG=${HAMA_HOME}/conf
# The Hama configuration (as defined in hama-site.xml)
BSP_MASTER_ADDRESS=localhost:40000
HAMA_ZOOKEEPER_QUORUM=localhost
# true, if you want Hama to split the input (one split per task)
BSP_SPLIT_INPUT=


# Optional: Spark configuration. Supports versions 1.*, and 2.0.0
# For Spark 2.*, use: mvn -Dspark2
# Spark versions 0.8.1, 0.9.0, and 0.9.1 are supported by MRQL 0.9.0 only.
# You may use the Spark prebuilts bin-hadoop1 or bin-hadoop2 (Yarn)
# Tested in local, standalone deploy, and Yarn modes
SPARK_HOME=${HOME}/spark
# URI of the Spark master node:
#   to run Spark on Standalone Mode, set it to spark://`hostname`:7077
#   to run Spark on a YARN cluster, set it to "yarn-client"
SPARK_MASTER=yarn-client
# Memory for Master (e.g. 1000M, 2G)
SPARK_MASTER_MEMORY=512M
# The default number of cores. For a Yarn cluster, set it to the number of available containers minus 1.
#   For local/standalone mode, set it to 2. It can be changed with the MRQL parameter -nodes.
SPARK_EXECUTOR_INSTANCES=2
# Number of cores for each worker. For Yarn, it is the number of cores per container.
SPARK_EXECUTOR_CORES=1
# Memory per Worker (e.g. 1000M, 2G)
SPARK_EXECUTOR_MEMORY=1G


# Optional: Flink configuration. Supports version 1.0.2 and 1.0.3
FLINK_VERSION=1.1.2
# Flink installation directory
FLINK_HOME=${HOME}/ap/flink-${FLINK_VERSION}
# number of slots per TaskManager (typically, the number of cores per node)
FLINK_SLOTS=4
# memory per TaskManager
FLINK_TASK_MANAGER_MEMORY=2048



# STORM CONFIGURATIONS
STORM_VERSION=1.0.2

#Strom installation directory
STORM_HOME=${HOME}/apache-storm-${STORM_VERSION}


# Claspaths

HAMA_JAR=${HAMA_HOME}/hama-core-${HAMA_VERSION}.jar
FLINK_JARS=.
for I in ${FLINK_HOME}/lib/*.jar; do
    FLINK_JARS=${FLINK_JARS}:$I
done

STORM_JARS=.
for I in ${STORM_HOME}/lib/*.jar; do
    
    if [[ $I != *"log4j-over-slf4j-"* ]]
    then
      STORM_JARS=${STORM_JARS}:$I
    fi
done

# YARN-enabled assembly jar
if [[ -d ${SPARK_HOME}/assembly/target ]]; then
   SPARK_JARS=`ls ${SPARK_HOME}/assembly/target/scala-*/*.jar`
else if [[ -d ${SPARK_HOME}/lib ]]; then
   SPARK_JARS=`ls ${SPARK_HOME}/lib/spark-assembly-*.jar`
else if [[ -d ${SPARK_HOME}/jars ]]; then
   SPARK_JARS=`ls ${SPARK_HOME}/jars/spark-core*.jar`
fi
fi
fi

HADOOP_JARS=`${HADOOP_HOME}/bin/hadoop classpath`

if [[ !(-f ${CUP_JAR}) ]]; then
   echo "*** Cannot find the parser generator CUP jar file. Need to edit mrql-env.sh"; exit -1
fi

if [[ !(-f ${JLINE_JAR}) ]]; then
   echo "*** Cannot find the JLine jar file. Need to edit mrql-env.sh"; exit -1
fi

export MRQL_HOME="$(cd `dirname $0`/..; pwd -P)"
if [[ !(-f `echo ${MRQL_HOME}/lib/mrql-core-*.jar`) ]]; then
   echo "*** Need to compile MRQL first using 'mvn clean install'"; exit -1
fi
