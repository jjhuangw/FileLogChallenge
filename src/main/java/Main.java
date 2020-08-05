import java.util.List;

import org.apache.spark.sql.SparkSession;

import executor.InputExecutor;

public class Main {

	public static void main(String[] args) {

		try {
			// This is local mode only for development, we can deploy on cluster for better performance.
			// We can assign the different resource base on requirement and specification.
			SparkSession sparkSession = SparkSession.builder().appName("Text File Data Load").master("local[*]")
					.config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
					.config("spark.executor.memory", "1g").config("spark.driver.memory", "1g").getOrCreate();

			InputExecutor executor = new InputExecutor(sparkSession);
			List<String> records;
			if (args.length == 2) {
				records = executor.execute(Integer.valueOf(args[0]), args[1]);
			} else {
				records = executor.execute(Integer.valueOf(args[0]), System.in);
			}
			for (String record : records) {
				System.out.println(record);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
