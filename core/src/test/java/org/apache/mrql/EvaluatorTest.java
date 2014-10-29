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
import java.io.StringReader;

import org.junit.BeforeClass;

import junit.framework.TestCase;

public abstract class EvaluatorTest extends TestCase {
	abstract protected Evaluator createEvaluator() throws Exception;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ClassImporter.load_classes();
		new TopLevel();
	}

	public void setUp() throws Exception {
		createEvaluator();		
		Translator.global_reset();
	}

	public void tearDown() throws IOException {
		if (Config.compile_functional_arguments)
			Compiler.clean();
                Config.max_bag_size_print = -1;
                if (Config.hadoop_mode) {
                    Plan.clean();
                    Evaluator.evaluator.shutdown(Plan.conf);
                }
	}

	private MRData execute(String query) throws Exception {
		evaluate("store ret := " + query);
		return getVariable("ret");
	}

	private MRData getVariable(String name) {
		return Interpreter.variable_lookup(name, Interpreter.global_env);
	}

	private void evaluate(String query) throws Exception {
		MRQLParser parser = new MRQLParser(new MRQLLex(new StringReader(query)));
		MRQLLex.reset();
		parser.parse();
	}

	public void testCore1Array() throws Exception {
		assertEquals(3, ((MR_int) execute("[1,2,3,4][2];")).get());
	}

	public void testCore1Range() throws Exception {
		Bag result = ((Bag) execute("[1..1000][10:20];"));
		assertEquals(0, result.size());
	}

	public void testCore1ArrayPlus() throws Exception {
		Bag result = ((Bag) execute("[1,2]+[1,2,3];"));
		assertEquals(5, result.size());
		assertEquals(1, ((MR_int) result.get(0)).get());
		assertEquals(2, ((MR_int) result.get(1)).get());
		assertEquals(1, ((MR_int) result.get(2)).get());
		assertEquals(2, ((MR_int) result.get(3)).get());
		assertEquals(3, ((MR_int) result.get(4)).get());
	}

	public void testCore1TupleIndex() throws Exception {
		assertEquals(1, ((MR_int) execute("{(\"a\",1),(\"b\",2)}[\"a\"];")).get());
	}

	public void testCore1Variable() throws Exception {
		evaluate("store xx := Node(\"a\",{(\"x\",\"1\")},[Node(\"b\",{},[CData(\"text\")])]);");
		MR_int result2 = (MR_int) execute("toInt(xx.@x)+1;");
		assertEquals(2, result2.get());
	}

	public void testCore1Cast() throws Exception {
		assertEquals(1L, ((MR_long) execute("1 as long;")).get());
		assertEquals(1.0, ((MR_double) execute("1 as double;")).get());
	}

	public void testCore1Math() throws Exception {
		assertEquals(1, ((MR_int) execute("min({1,2,3});")).get());
		assertEquals(3.4, ((MR_double) execute("max({1.2,3.4});")).get());

		assertEquals((double) 1.0F, ((MR_double) execute("min({1.0 as double,4.8 as double});")).get());
		assertEquals(3, ((MR_long) execute("count({1,2,3});")).get());

		assertEquals(1.2 + 3.4, ((MR_double) execute("sum({1.2,3.4});")).get());
		assertEquals(2.0, ((MR_double) execute("avg({1,2,3});")).get());
		assertEquals((1.2 + 3.4) / 2, ((MR_double) execute("avg({1.2,3.4});")).get());
		assertEquals((1.3F + 4.8F) / 2, (float)((MR_double) execute("avg({1.3 as float,4.8 as float});")).get());
	}
}
