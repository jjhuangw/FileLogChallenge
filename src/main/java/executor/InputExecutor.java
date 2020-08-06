package executor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
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
public class InputExecutor implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private SparkSession sparkSession;

	public InputExecutor(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Find the X-largest values in data
	 *
	 * @param limit  value
	 * @param system input stream
	 */
	public List<String> execute(int limit, InputStream inputStream) throws IOException {
		long start = System.currentTimeMillis();
		// TODO: if streaming input was over a server limitation, we can save data into S3 or HDFS storage without keeping in memory 
		//       and use cluster mode to process it.   
		List<Row> rows = new ArrayList<>();
		try (BufferedReader bufReader = new BufferedReader(new InputStreamReader(inputStream))) {
			String inputStr = null;
			while ((inputStr = bufReader.readLine()) != null) {
				rows.add(parseLine(inputStr));
			}
		}
		Dataset<Row> df = sparkSession.createDataFrame(rows, createAndGetSchema());
		List<String> records = findTheLargestValues(limit, df);
		System.out.println("elasped time:" + ((System.currentTimeMillis() - start)/ 1000) % 60 + " seconds");
		return records;
	}

	/**
	 * Find the X-largest values in data
	 *
	 * @param the largest value count
	 * @param absolute file path
	 */
	public List<String> execute(int limit, String filePath) {
		long start = System.currentTimeMillis();
		Dataset<Row> df = sparkSession.read().textFile(filePath).map((MapFunction<String, Row>) line -> parseLine(line),
				RowEncoder.apply(createAndGetSchema()));
		List<String> records = findTheLargestValues(limit, df);
		System.out.println("elasped time:" + ((System.currentTimeMillis() - start)/ 1000) % 60 + " seconds");
		return records;
	}

	/**
	 * Parse line and transform to structure
	 *
	 * @param the largest value count
	 * @return Row structure
	 */
	public Row parseLine(String line) {
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
	public List<String> findTheLargestValues(int limit, Dataset<Row> df) {
		return df.select(df.col("uid")).orderBy(df.col("value").desc()).limit(limit)
				.as(Encoders.STRING()).collectAsList();
	}

	/**
	 * Create data schema
	 *
	 * @param data schema
	 * 
	 */
	public StructType createAndGetSchema() {
		return new StructType(new StructField[] { new StructField("uid", DataTypes.StringType, false, Metadata.empty()),
				new StructField("value", DataTypes.DoubleType, false, Metadata.empty()) });
	}
}
