package executor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Input Executor
 *
 * @author chienchang.huang
 * 
 */
public class InputExecutor {

	private SparkSession sparkSession;

	public InputExecutor(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}
	
	/**
	 * Find the X-largest values in data
	 *
	 * @param limit value
	 * @param system input stream
	 */
	public void execute(int limit, InputStream inputStream) throws IOException {
		List<Row> rows = new ArrayList<>();
		try (BufferedReader bufReader = new BufferedReader(new InputStreamReader(inputStream))){
			String inputStr = null;
			while ((inputStr = bufReader.readLine()) != null) {
				rows.add(parseLine(inputStr));
			}
		}
		Dataset<Row> df = sparkSession.createDataFrame(rows, createAndGetSchema());
		findTheLargetValues(limit, df);
	}
	
	/**
	 * Find the X-largest values in data
	 *
	 * @param the largest value count
	 * @param absolute file path
	 */
	public void execute(int limit, String filePath) {
		Dataset<Row> df = sparkSession.read().textFile(filePath)
				.map((MapFunction<String, Row>) line -> parseLine(line), RowEncoder.apply(createAndGetSchema()));
		findTheLargetValues(limit, df);
	}
	
	/**
	 * Parse line and transform to structure
	 *
	 * @param the largest value count
	 * @return Row structure
	 */
	private Row parseLine(String line) {
		String[] parts = line.split("\\s+");
		return RowFactory.create((parts[0]), Double.parseDouble(parts[1]));
	}

	/**
	 * Find the largest values
	 *
	 * @param the largest value count
	 * @param dataframe
	 * 
	 */
	private void findTheLargetValues(int limit, Dataset<Row> df) {
		List<String> list = df.select(df.col("uid")).orderBy(df.col("value").desc()).limit(limit).as(Encoders.STRING())
				.collectAsList();
		for (String s : list) {
			System.out.println(s);
		}
	}

	private StructType createAndGetSchema() {
		return new StructType(
				new StructField[] { new StructField("uid", DataTypes.StringType, false, Metadata.empty()),
						new StructField("value", DataTypes.DoubleType, false, Metadata.empty()) });
	}
}
