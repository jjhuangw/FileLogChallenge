package executor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;

public class Tester {

	public static void main(String[] args) throws UnsupportedEncodingException, FileNotFoundException, IOException {
		int count = 100;
		File file = new File("/Users/chienchang.a.huang/eclipse-workspace/mng-api/find-largest-value/"+count+".txt");
		file.getParentFile().mkdir();
		file.createNewFile();
		try (BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(file), "utf-8"))) {
			writer.write("1426828028 350");
			writer.newLine();
			writer.write("1426828056 231");
			writer.newLine();
			for(int i = 0 ; i < count-3 ; i++) {
				writer.write(System.currentTimeMillis()/1000 + " 9");
				writer.newLine();
			}
			writer.write("1426828068 300");
			writer.newLine();
		}
		System.out.println("records = " + count + ", size=" + Files.size(file.toPath())/ 1024 + " kb");
	}

}
