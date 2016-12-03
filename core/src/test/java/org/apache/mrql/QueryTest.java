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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.FileReader;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.apache.log4j.*;
import java.util.Enumeration;

public abstract class QueryTest extends TestCase {
	private static String TEST_QUERY_DIR = "../tests/queries";
	private static String TEST_RESULT_DIR = "../tests/results";
	protected File queryDir;
	protected File resultDir;
	private static Evaluator evaluator;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ClassImporter.load_classes();
		new TopLevel();
	}

	public void setUp() throws Exception {
		queryDir = new File(TEST_QUERY_DIR);
		resultDir = new File(TEST_RESULT_DIR);
		resultDir.mkdirs();

		// if(evaluator==null) // the spark evaluator needs to be recreated
			evaluator = createEvaluator();
		Translator.global_reset();
                for ( Enumeration en = LogManager.getCurrentLoggers(); en.hasMoreElements(); )
                    ((Logger)en.nextElement()).setLevel(Level.ERROR);
                LogManager.getRootLogger().setLevel(Level.ERROR);
	}

	public void tearDown() throws IOException {
		if (Config.compile_functional_arguments)
			Compiler.clean();
                if (Config.hadoop_mode) {
                    Plan.clean();
                    Evaluator.evaluator.shutdown(Plan.conf);
                }
	}
	
	abstract protected Evaluator createEvaluator() throws Exception;

	public void testCore() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "core_1.mrql"), resultDir));
	}

	public void testDistinct() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "distinct_1.mrql"), resultDir));
	}

	public void testFactorization() throws Exception {
            if (!Config.bsp_mode) // matrix factorization needs at least 3 nodes in BSP Hama mode
		assertEquals(0, queryAndCompare(new File(queryDir, "factorization_1.mrql"), resultDir));
	}

	public void testGroupBy() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "group_by_1.mrql"), resultDir));
	}
	
	public void testGroupByHaving() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "group_by_having_1.mrql"), resultDir));
	}	

	public void testGroupByOrderBy() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "group_by_order_by_1.mrql"), resultDir));
		assertEquals(0, queryAndCompare(new File(queryDir, "group_by_order_by_2.mrql"), resultDir));
	}

	public void testJoinGroupBy() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "join_group_by_1.mrql"), resultDir));
		assertEquals(0, queryAndCompare(new File(queryDir, "join_group_by_2.mrql"), resultDir));
		assertEquals(0, queryAndCompare(new File(queryDir, "join_group_by_3.mrql"), resultDir));
	}
	
	public void testJoinOrderBy() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "join_order_by_1.mrql"), resultDir));
	}

	public void testJoins() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "joins_1.mrql"), resultDir));
	}

	public void testKmeans() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "kmeans_1.mrql"), resultDir));
	}

	public void testLoop() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "loop_1.mrql"), resultDir));
	}

	public void testMatrix() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "matrix_1.mrql"), resultDir));
	}

	public void testNestedSelect() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "nested_select_1.mrql"), resultDir));
	}

	public void testOrderBy() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "order_by_1.mrql"), resultDir));
	}

	public void testPagerank() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "pagerank_1.mrql"), resultDir));
	}

	public void testRelationalJoin() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "relational_join_1.mrql"), resultDir));
	}

	public void testTotalAggregation() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "total_aggregation_1.mrql"), resultDir));
		assertEquals(0, queryAndCompare(new File(queryDir, "total_aggregation_2.mrql"), resultDir));
	}

	public void testUdf() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "udf_1.mrql"), resultDir));
	}

	public void testUserAggregation() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "user_aggregation_1.mrql"), resultDir));
	}

	public void testXml() throws Exception {
		assertEquals(0, queryAndCompare(new File(queryDir, "xml_1.mrql"), resultDir));
		assertEquals(0, queryAndCompare(new File(queryDir, "xml_2.mrql"), resultDir));
	}

    protected int queryAndCompare ( File query, File resultDir ) throws Exception {
        System.err.println("Testing "+query);
        Translator.global_reset();
        String qname = query.getName();
        qname = qname.substring(0,qname.length()-5);
        String resultFile = resultDir.getAbsolutePath()+"/"+qname+".txt";
        boolean exists = new File(resultFile).exists();
        if (exists)
            System.setOut(new PrintStream(resultDir.getAbsolutePath()+"/"+qname+"_result.txt"));
        else System.setOut(new PrintStream(resultFile));
        Config.max_bag_size_print = -1;
        Config.testing = true;
        MRQL.evaluate("store NaN := -1;");
        MRQLLex scanner = new MRQLLex(new FileInputStream(query));
        MRQLParser parser = new MRQLParser(scanner);
        parser.setScanner(scanner);
        MRQLLex.reset();
        parser.parse();
        PrintStream errorStream = System.err;
        int i;
        if (exists && (i = compare(resultFile,resultDir.getAbsolutePath()+"/"+qname+"_result.txt")) > 0) {
            return i;
        } else if (exists) {
            return 0;
        } else {
            return 0;
        }
    }

    private int compare2(String file1, String file2) throws Exception {
        FileInputStream s1 = new FileInputStream(file1);
        FileInputStream s2 = new FileInputStream(file2);
        int b1, b2;
        int i = 1;
        while ((b1 = s1.read()) == (b2 = s2.read()) && b1 != -1 && b2 != -1)
            i++;
        return (b1 == -1 && b2 == -1) ? 0 : i;
    }

    private String read_lines ( BufferedReader reader ) throws Exception {
        String s = "";
        boolean even = true;
        do {
            String line = reader.readLine();
            // ignore Flink tracing output
            while (line != null && line.contains(" switched to "))
                line = reader.readLine();
            if (line == null)
                return s;
            for ( int i = 0; i < line.length(); i++ )
                if (line.charAt(i) == '\"')
                    even = !even;
            s += line;
        } while (!even);
        return s;
    }

    private int compare ( String file1, String file2 ) throws Exception {
        BufferedReader reader1 = new BufferedReader(new FileReader(file1));
        BufferedReader reader2 = new BufferedReader(new FileReader(file2));
        String line1 = read_lines(reader1);
        String line2 = read_lines(reader2);
        int i = 1;
        do {
            boolean hm = Config.hadoop_mode;
            Config.hadoop_mode = false;
            MRData v1 = MRQL.query(line1);
            MRData v2 = MRQL.query(line2);
            Config.hadoop_mode = hm;
            if (!equal_value(v1,v2)) {
                System.err.println("*** "+file1+" (query "+i+"):\nFound: "+v1+"\nExpected: "+v2);
                return i;
            };
            line1 = read_lines(reader1);
            line2 = read_lines(reader2);
            i++;
        } while (line1 != "" && line2 != "");
        return 0;
    }

    private boolean member ( MRData e, Bag v ) {
        for ( MRData x: v )
            if (equal_value(x,e))
                return true;
        return false;
    }

    private boolean equal_value ( MRData v1, MRData v2 ) {
        if (v1 instanceof Bag && v2 instanceof Bag) {
            Bag b1 = (Bag)v1;
            Bag b2 = (Bag)v2;
            if (b1.size() != b2.size())
                return false;
            for ( MRData e1: b1 )
                if (!member(e1,b2))
                    return false;
            for ( MRData e2: b2 )
                if (!member(e2,b1))
                    return false;
            return true;
        } else if (v1 instanceof Tuple && v2 instanceof Tuple) {
            Tuple t1 = (Tuple)v1;
            Tuple t2 = (Tuple)v2;
            if (t1.size() != t2.size())
                return false;
            for ( short i = 0; i < t1.size(); i++ )
                if (!equal_value(t1.get(i),t2.get(i)))
                    return false;
            return true;
        } else if (v1 instanceof Union && v2 instanceof Union) {
            Union t1 = (Union)v1;
            Union t2 = (Union)v2;
            return t1.tag() == t2.tag() && equal_value(t1.value(),t2.value());
        } else if (v1 instanceof MR_double && v2 instanceof MR_double)
            return (float)((MR_double)v1).get() == (float)((MR_double)v2).get();
        else return v1.equals(v2);
    }
}
