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
package org.apache.mrql;

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class StormQueryLocalModeTest extends QueryTest {

	public void tearDown() throws IOException {
		super.tearDown();
		Plan.clean();
	}

	@Override
	protected Evaluator createEvaluator() throws Exception {
		Configuration conf = new Configuration();

		Config.bsp_mode = false;
		Config.spark_mode = false;
		Config.flink_mode = false;
		Config.storm_mode = true;
		Config.map_reduce_mode = false;

		Evaluator.evaluator = new StormEvaluator();

		Config.quiet_execution = true;

		String[] args = new String[] { "-dist", "-storm","-stream","5" };

		conf = Evaluator.evaluator.new_configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();

		args = gop.getRemainingArgs();
		
		Config.hadoop_mode = true;
		Config.testing = true;		
		Config.parse_args(args, conf);

		Evaluator.evaluator.init(conf);
		
		return Evaluator.evaluator;
	}

	@Override 
	public void testCore() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "core_1.mrql"), resultDir));
	}

	@Override 
	public void testDistinct() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "distinct_1.mrql"), resultDir));
	}

	@Override 
	public void testFactorization() throws Exception {
           // if (!Config.bsp_mode) // matrix factorization needs at least 3 nodes in BSP Hama mode
		//assertEquals(0, queryAndCompare(new File(queryDir, "factorization_1.mrql"), resultDir));
	}

	@Override
	public void testGroupBy() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "group_by_1.mrql"), resultDir));
	}
	
	@Override 
	public void testGroupByHaving() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "group_by_having_1.mrql"), resultDir));
	}	

	@Override 
	public void testGroupByOrderBy() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "group_by_order_by_1.mrql"), resultDir));
		//assertEquals(0, queryAndCompare(new File(queryDir, "group_by_order_by_2.mrql"), resultDir));
	}

	@Override 
	public void testJoinGroupBy() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "join_group_by_1.mrql"), resultDir));
		//assertEquals(0, queryAndCompare(new File(queryDir, "join_group_by_2.mrql"), resultDir));
		//assertEquals(0, queryAndCompare(new File(queryDir, "join_group_by_3.mrql"), resultDir));
	}
	
	@Override 
	public void testJoinOrderBy() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "join_order_by_1.mrql"), resultDir));
	}

	@Override 
	public void testJoins() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "joins_1.mrql"), resultDir));
	}

	@Override 
	public void testKmeans() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "kmeans_1.mrql"), resultDir));
	}

	@Override 
	public void testLoop() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "loop_1.mrql"), resultDir));
	}

	@Override 
	public void testMatrix() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "matrix_1.mrql"), resultDir));
	}

	@Override 
	public void testNestedSelect() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "nested_select_1.mrql"), resultDir));
	}

	@Override 
	public void testOrderBy() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "order_by_1.mrql"), resultDir));
	}

	@Override
	public void testPagerank() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "pagerank_1.mrql"), resultDir));
	}

	@Override 
	public void testRelationalJoin() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "relational_join_1.mrql"), resultDir));
	}

	@Override 
	public void testTotalAggregation() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "total_aggregation_1.mrql"), resultDir));
		//assertEquals(0, queryAndCompare(new File(queryDir, "total_aggregation_2.mrql"), resultDir));
	}

	@Override 
	public void testUdf() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "udf_1.mrql"), resultDir));
	}

	@Override 
	public void testUserAggregation() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "user_aggregation_1.mrql"), resultDir));
	}

	@Override 
	public void testXml() throws Exception {
		//assertEquals(0, queryAndCompare(new File(queryDir, "xml_1.mrql"), resultDir));
	//	assertEquals(0, queryAndCompare(new File(queryDir, "xml_2.mrql"), resultDir));
	}
}
