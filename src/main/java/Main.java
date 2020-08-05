import java.util.InputMismatchException;
import java.util.Scanner;

import org.apache.spark.sql.SparkSession;

import executor.FileExecutor;

public class Main {

	public static void main(String[] args) {
		
		// This is local mode only for development, we can deploy on cluster for better performance.
		// We can assign the different resource base on requirement and resource.
		SparkSession sparkSession = SparkSession.builder().appName("Text File Data Load").master("local[*]")
				.config("spark.driver.host", "127.0.0.1")
				.config("spark.driver.bindAddress", "127.0.0.1")
				.config("spark.executor.memory", "1g")
				.config("spark.driver.memory", "1g")
				.getOrCreate();

		Scanner scan = null;
		try {
			scan = new Scanner(System.in);
			System.out.println("Please choose file input mode from these choices");
			System.out.println("-------------------------\n");
			System.out.println("1 - stdin");
			System.out.println("2 - provide the absolute file path");
			System.out.println("3 - Quit");
			int selection = scan.nextInt();
			System.out.println("-------------------------\n");
			
			int limit;
			
			switch (selection) {
			case 1:
				System.out.println("please input the limit");
				limit = scan.nextInt();
				System.out.println("please input your content");
				System.out.println(scan.next());
				break;
			case 2:
				System.out.println("please input the limit");
				limit = scan.nextInt();
				System.out.println("please input the absolute file path");
				String filePath = scan.next();
				long start = System.currentTimeMillis();
				FileExecutor executor = new FileExecutor(sparkSession);
				executor.execute(limit, filePath);
				long finish = System.currentTimeMillis();
				System.out.println("elasped time: " + (finish - start));
				break;
			case 3:
			default:
			}
		}catch(InputMismatchException e) {
			System.out.println("Input format is incorrect! " + e.getMessage());
		}finally {
			if(scan!=null) scan.close();
			System.exit(0);
		}
	}

}
