import org.apache.spark.sql.SparkSession;

import executor.InputExecutor;

public class Main {

	public static void main(String[] args) {

		try {
			// This is local mode only for development, we can deploy on cluster for better performance.
			// We can assign the different resource base on requirement and resource.
			SparkSession sparkSession = SparkSession.builder().appName("Text File Data Load").master("local[*]")
					.config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
					.config("spark.executor.memory", "1g").config("spark.driver.memory", "1g").getOrCreate();

			InputExecutor executor = new InputExecutor(sparkSession);
			
			if (args.length == 2) {
				executor.execute(Integer.valueOf(args[0]), args[1]);
			} else {
				executor.execute(Integer.valueOf(args[0]), System.in);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
