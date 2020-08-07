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
					.config("spark.executor.memory", "512m").config("spark.driver.memory", "512m").getOrCreate();

			InputExecutor executor = new InputExecutor(sparkSession);
			List<String> records;
			int limit = Integer.valueOf(args[0]);
			
			if (args.length == 2) {
				records = executor.execute(limit, args[1]);
			} else if(args.length == 1) {
				records = executor.execute(limit, System.in);
			}else {
				 throw new Exception("Only allow at most 2 arguments, please check REAMME.");
			}
			
			for (String record : records) {
				System.out.println(record);
			}
			
		} catch(NumberFormatException ne) {
			System.out.println("");
			System.err.println("The first argument is the largest values count, it only accept numeric input.");
			System.out.println("");
		} catch (Exception e) {
			System.out.println("");
			e.printStackTrace();
			System.out.println("");
			System.exit(1);
		}
	}

}
