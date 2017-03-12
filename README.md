# BDPA_Assign2_HJACOME
Set-Similarity Joins

## Pre-processing
The goal is to pre-process the document corpus of pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt) to have only lines of unique words sorted by global frequency, excluding stopwords and clear of special characters.

To this end, we use the stopwords.csv file from assignment 1 as well as the wordcount.txt file from assignment 0, with format "word#count" for parsing purposes.

We also implement a counter that fetches the total number of lines and saves it on HDFS.

### Main
```java
public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "PreProcessing");
		job.setJarByClass(PreProcessing.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		Counter counter = job.getCounters().findCounter(CustomCounters.NUMLINES);
		System.out.println("Number of lines : " + counter.getValue());

		FileSystem hdfs = FileSystem.get(URI.create("count"), conf);
		Path file = new Path("num_lines.txt");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write("Number of lines : " + counter.getValue());
		br.close();
		hdfs.close();
}

```

### Mapper

### Reducer
