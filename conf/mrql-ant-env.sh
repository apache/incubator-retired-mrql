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
# Used by Makefile and build.xml only
#
#--------------------------------------------------------------------------------

# Required: The MRQL version
MRQL_VERSION=0.9.0-incubating

# Required: The jflex parser library
# You may install it as a linux package or download it from http://jflex.de/
JFLEX_JAR=/usr/share/java/JFlex.jar

# Required: The CUP parser library
# You may install it as a linux package or download it from http://www2.cs.tum.edu/projects/cup/
CUP_JAR=/usr/share/java/cup.jar

# Required: The JLine library
# You may download from http://jline.sourceforge.net
JLINE_JAR=${HOME}/mrql18/lib/jline-1.0.jar

MRQL_CLASSPATH=classes:${HAMA_JAR}:${SPARK_JARS}:${HADOOP_JARS}
