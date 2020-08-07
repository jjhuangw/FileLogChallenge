
# Find the X-largest values in the file or stdin stream
This implementation is using Java with Spark 2.4.3

- - -
## Goal:
Write a program that reads from 'stdin' the contents of a file, and optionally accepts the absolute path of a file from the command line. The file/stdin stream is expected to be in the following format. The output should be a list of the unique ids associated with the X-largest values in the rightmost column, where X is specified by an input parameter.  

\[unique record identifier\] \[white_space\] \[numeric value\]  
e.g.  
1426828011 9  
1426828028 350  
1426828037 25  
1426828056 231  
1426828058 109  
1426828066 111  

For example, given the input data above and X=3, the following would be valid output:  
1426828028  
1426828066  
1426828056  

- - -
## How to execute:

- **Build jar package** - Please install java8/maven on your local before start

```
# goto the project
cd find-largest-value
# build project
mvn clean package
# will generate the jar file in target folder
ll target/
```
- **Run application** - Read content from stdin

```
# java -jar target/find-largest-value-jar-with-dependencies.jar $largest_value_count file_streaming_input
java -jar target/find-largest-value-jar-with-dependencies.jar 3 </Users/chienchang.a.huang/test.txt
```
- **Run application** - Read from file path

```
# java -jar target/find-largest-value-jar-with-dependencies.jar $largest_value_count absolute_file_path
java -jar target/find-largest-value-jar-with-dependencies.jar 3 /Users/chienchang.a.huang/test.txt
```
## Performance test result: 
local pc spec as following,
- cpu: 2.3 GHz Intel Core i5, 2 cores
- memory: 16 GB 2133 MHz LPDDR3  
- spark executor memory: 512 MB
- spark driver memory: 512 MB  

performance result in local test:
1. 1000000 records, 12.695 MB, find the top 3 value  
  -- total execution time: 5 seconds (5277 milliseconds) and used 2.1 GB memory
2. 10000000 records, 126.95 MB, find the top 3 value  
  -- total execution time: 8 seconds (8870 milliseconds) and used 2.1 GB memory
3. 100000000 records, 1.269531 GB, find the top 3 value  
  -- total execution time: 46 seconds (46366 milliseconds) and used 2.1 GB memory
4. 1000000000 records, 12.69531 GB, find the top 3 value  
  -- total execution time: 425.35200 seconds (425352 milliseconds) and used 2.1 GB memory  
  
** If ran in cluster, the performance will have significant improvement
