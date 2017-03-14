# BDPA Assignment 2 Set-Similarity Joins
###### Hippolyte JACOMET

![becausewhynot.jpg](figures/becausewhynot.jpg)

## Pre-processing step
The goal is to pre-process the document corpus of pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt) to have only lines of unique words sorted by global frequency, excluding stopwords and clear of special characters.

To this end, we use the stopwords.csv file from assignment 1 as well as the wordcount.txt file from assignment 0, with key-value separator set to "#" for parsing purposes.

We also implement a counter that fetches the total number of lines and saves it on HDFS.


### Method
Our approach is the following:
- Mapper: First implement a setup class that reads the stopwords.csv file and stores it in a single string. For each line, the `mapper` then writes a key-word pair for each word based on the aformentionned conditions.
- Reducer: First implement a setup class that reads the wordcount.txt file and stores its (word, count) pairs in a hashmap. For each key, the ``reducer`` then sorts the values (here, the filterd words of a line) using the wordcount hashmap, and saves them in a string.

#### Main
```java
static enum CustomCounters {NUMLINES}
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "InvertedIndex");
	job.setJarByClass(PreProcessing.class);

	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ":");
	job.setNumReduceTasks(1);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	conf.set("mapreduce.map.output.compress", "true");
	conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");


	job.waitForCompletion(true);
	Counter counter = job.getCounters().findCounter(CustomCounters.NUMLINES);

	FileSystem hdfs = FileSystem.get(URI.create("count"), conf);
	Path file = new Path("counter.txt");
	if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
	OutputStream os = hdfs.create(file);
	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	br.write("Unique words in a single file = " + counter.getValue());
	br.close();
	hdfs.close();

	System.out.println("Number of lines = " + counter.getValue());
}
```

#### Mapper
```java
public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

}
```
#### Reducer
```java
public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

}
```
## Set-similarity joins

### Brute force approach
Here we perform all pairwise comparisons. The mapper should output as key a pair of two line numbers and as value the words from the first line. A hashset seems to be appropriate for this because the order of the pair shouldn't matter.
Since Hadoop native Writable types are only based on java's primitive types, we need to create our own, implementing the WritableComparable method.

https://vangjee.wordpress.com/2012/03/30/implementing-rawcomparator-will-speed-up-your-hadoop-mapreduce-mr-jobs-2/

### Indexing approach
Damn that's awesome.

### Comparing the two methods
So surprising.

## Conclusion
Some crazy stuff over here.
