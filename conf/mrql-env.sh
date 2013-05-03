#!/bin/bash
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


# The java implementation to use.  Required.
JAVA_HOME=/root/jdk

# The Hadoop installation directory  Required.
HADOOP_VERSION=1.0.3
HADOOP_HOME=${HOME}/hadoop-${HADOOP_VERSION}

# Optional: the Hama installation directory
HAMA_VERSION=0.5.0
HAMA_HOME=${HOME}/hama-${HAMA_VERSION}
HAMA_JAR=${HAMA_HOME}/hama-core-${HAMA_VERSION}.jar

# The Hadoop configuration directory.  Required.
HADOOP_CONF=${MRQL_HOME}/conf/conf-hadoop

# Optional: the Hama configuration directory
HAMA_CONF=${MRQL_HOME}/conf/conf-hama

# The CUP parser library.
# You may install it as a linux package or download it from http://www2.cs.tum.edu/projects/cup/
CUP_JAR=/usr/share/java/cup.jar

# Hadoop libraries are from a Hadoop tarball
HADOOP_JARS=${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/lib/commons-cli-1.2.jar

# Hadoop libraries are from a YARN tarball
#HADOOP_JARS=${HADOOP_HOME}/share/hadoop/common/hadoop-common-$HADOOP_VERSION.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-$HADOOP_VERSION.jar:${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-$HADOOP_VERSION.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-$HADOOP_VERSION.jar:${HADOOP_HOME}/share/hadoop/common/lib/commons-cli-1.2.jar

# Hadoop libraries are from a Cloudera package
#HADOOP_JARS=/usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hadoop-0.20/lib/commons-cli-1.2.jar

MRQL_CLASSPATH=classes:${HAMA_JAR}:${HADOOP_JARS}
