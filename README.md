# BDPA_Assign2_HJACOME
Set-Similarity Joins

![hello.jpg](figures/test.jpg)


## Pre-processing step
The goal is to pre-process the document corpus of pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt) to have only lines of unique words sorted by global frequency, excluding stopwords and clear of special characters.

To this end, we use the stopwords.csv file from assignment 1 as well as the wordcount.txt file from assignment 0, with key-value separator set to "#" for parsing purposes.

We also implement a counter that fetches the total number of lines and saves it on HDFS.

### Procedure
Our approach is the following:
- Mapper: First implement a setup class that reads the stopwords.csv file and stores it in a single string. For each line, the mapper then writes a key-word pair for each word based on the aformentionned conditions.
- Reducer: First implement a setup class that reads the wordcount.txt file and stores its (word, count) pairs in a hashmap. For each key, the reducer then sorts the values (here, the filterd words of a line) using the wordcount hashmap, and saves them in a string.

### Main
```java
public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

}
```

### Mapper
```java
public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

}
```
### Reducer
```java
public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

}
```
## Set-similarity joins

### Brute force approach
Here we perform all pairwise comparisons. The mapper should output as key a pair of two line numbers and as value the words from the first line. A hashset seems to be appropriate for this because the order of the pair shouldn't matter.
Since Hadoop native Writable types are only based on java's primitive types, we need to create our own, based on the ArrayWritable found at:
https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/ArrayWritable.java.
