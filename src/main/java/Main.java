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
			int limit = Integer.valueOf(args[0]);
			
			if (args.length == 2) {
				records = executor.execute(limit, args[1]);
			} else if(args.length == 1) {
				records = executor.execute(limit, System.in);
			}else {
				 throw new Exception("Only allow at most 2 arguments");
			}
			
			for (String record : records) {
				System.out.println(record);
			}
			
		} catch(NumberFormatException ne) {
			System.out.println("The first argument is the largest values count, it only accept numeric input.");
		}catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
