package executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class InputExecutorTest {

	private static SparkSession sparkSession;
	private static InputExecutor executor;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		sparkSession = SparkSession.builder().appName("SessionizeExecutor").master("local[*]")
				.config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate();
		executor = new InputExecutor(sparkSession);
	}
	
	@AfterAll
	public static void afterClass() {
		if (sparkSession != null) {
			sparkSession.stop();
		}
	}
	
	private List<Row> generateTestRow() {
		List<Row> rows = new ArrayList<>();
		Row r1 = RowFactory.create("1426828011", new Double(9));
		Row r2 = RowFactory.create("1426828028", new Double(350));
		Row r3 = RowFactory.create("1426828037", new Double(25));
		Row r4 = RowFactory.create("1426828056", new Double(231));
		Row r5 = RowFactory.create("1426828058", new Double(109));
		Row r6 = RowFactory.create("1426828066", new Double(111));
		rows.add(r1);
		rows.add(r2);
		rows.add(r3);
		rows.add(r4);
		rows.add(r5);
		rows.add(r6);
		return rows;
	}
	
	@Test
	public void testParseLine() {
		Dataset<Row> answer = sparkSession.createDataFrame(generateTestRow(), executor.createAndGetSchema());
		Row r1 = executor.parseLine("1426828011 9");
		Row r2 = executor.parseLine("1426828028     350");
		Row r3 = executor.parseLine("1426828037            25");
		Row r4 = executor.parseLine("1426828056 231");
		Row r5 = executor.parseLine("1426828058 109");
		Row r6 = executor.parseLine("1426828066 111");
		List<Row> rows = new ArrayList<>();
		rows.add(r1);
		rows.add(r2);
		rows.add(r3);
		rows.add(r4);
		rows.add(r5);
		rows.add(r6);
		Dataset<Row> testData = sparkSession.createDataFrame(generateTestRow(), executor.createAndGetSchema());
		assertEquals(0, answer.except(testData).count());
	}
	
	@Test
	public void testFindTheLargetValues() {
		Dataset<Row> df = sparkSession.createDataFrame(generateTestRow(), executor.createAndGetSchema());
		List<String> records = executor.findTheLargestValues(3, df);
		Set<String> test = new HashSet<>();
		test.add("1426828028");
		test.add("1426828066");
		test.add("1426828056");
		assertTrue(new HashSet(records).equals(test));
	}
	
	@Test
	public void testFindTheLargetValuesFromDuplicatedValue() {
		List<Row> rows = new ArrayList<>();
		Row r1 = RowFactory.create("1426828011", new Double(200));
		Row r2 = RowFactory.create("1426828028", new Double(200));
		Row r3 = RowFactory.create("1426828066", new Double(115));
		Row r4 = RowFactory.create("1426828037", new Double(110));
		rows.add(r1);
		rows.add(r2);
		rows.add(r3);
		rows.add(r4);
		Dataset<Row> df = sparkSession.createDataFrame(rows, executor.createAndGetSchema());
		List<String> records = executor.findTheLargestValues(2, df);
		Set<String> test = new HashSet<>();
		test.add("1426828011");
		test.add("1426828028");
		assertTrue(new HashSet(records).equals(test));
	}
	
	// If we have 2 uid are the same value but only want to find the 1 largest. 
	@Test
	public void testFindTheLargetValuesFromDuplicatedValue2() {
		List<Row> rows = new ArrayList<>();
		Row r1 = RowFactory.create("1426828011", new Double(200));
		Row r2 = RowFactory.create("1426828028", new Double(200));
		Row r3 = RowFactory.create("1426828066", new Double(115));
		Row r4 = RowFactory.create("1426828037", new Double(110));
		rows.add(r1);
		rows.add(r2);
		rows.add(r3);
		rows.add(r4);
		Dataset<Row> df = sparkSession.createDataFrame(rows, executor.createAndGetSchema());
		List<String> records = executor.findTheLargestValues(1, df);
		Set<String> test = new HashSet<>();
		test.add("1426828011");
		test.add("1426828028");
		assertTrue(CollectionUtils.containsAny(test, records));
	}
	
	@Test
	public void testExecuteFromFilePathInput() {
		File testFile = new File(InputExecutorTest.class.getClassLoader().getResource("test.txt").getFile());
		List<String> records = executor.execute(3, testFile.getAbsolutePath());
		Set<String> test = new HashSet<>();
		test.add("1426828028");
		test.add("1426828066");
		test.add("1426828056");
		assertTrue(new HashSet(records).equals(test));
	}
	
	@Test
	public void testExecuteFromSystemInput() throws Exception {
		File testFile = new File(InputExecutorTest.class.getClassLoader().getResource("test.txt").getFile());
		List<String> records = executor.execute(3, new FileInputStream(testFile));
		Set<String> test = new HashSet<>();
		test.add("1426828028");
		test.add("1426828066");
		test.add("1426828056");
		assertTrue(new HashSet(records).equals(test));
	}
}
