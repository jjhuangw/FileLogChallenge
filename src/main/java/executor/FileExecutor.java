package executor;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * File Executor
 *
 * @author chienchang.huang
 * 
 */
public class FileExecutor {

	private SparkSession sparkSession;

	public FileExecutor(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}
	
	
	
	
	public void execute(int limit, String filePath) {
		JavaRDD<Row> dataRDD = JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).textFile(filePath).map(line -> {
			String[] parts = line.split("\\s+");
			return RowFactory.create((parts[0]), Double.parseDouble(parts[1]));
		});
		
		Dataset<Row> df = sparkSession.createDataFrame(dataRDD, getSchema());
		List<String> list = df.select(df.col("uid")).orderBy(df.col("value").desc()).limit(limit).as(Encoders.STRING()).collectAsList();
		for(String s: list) {
			System.out.println(s);
		}
	}

	private StructType getSchema() {
		return new StructType(
				new StructField[] { new StructField("uid", DataTypes.StringType, false, Metadata.empty()),
						new StructField("value", DataTypes.DoubleType, false, Metadata.empty()) });
	}
}
