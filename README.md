
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
mvn clean package
```
- **Run application** - Read content from stdin

```
# java -jar target/find-large-value-jar-with-dependencies.jar $largest_value_count file_streaming_input
java -jar target/find-large-value-jar-with-dependencies.jar 3 < test.txt
```
- **Run application** - Read from file path

```
# java -jar target/find-large-value-jar-with-dependencies.jar $largest_value_count absolute_file_path
java -jar target/find-large-value-jar-with-dependencies.jar 3 test.txt
```
## Performance test result:

