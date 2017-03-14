package preprocessing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class PreProcessing{
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
		Path file = new Path("line_counter.txt");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write("Unique words in a single file = " + counter.getValue());
		br.close();
		hdfs.close();

		System.out.println("Number of lines = " + counter.getValue());
	}


	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

		String stopwords = new String();
		public void setup(Context context) throws IOException, InterruptedException {
//		Import stop words in a string

//		Test with local file for standalone mode
//			File file = new File("stopwords.csv");
//			Scanner sw = new Scanner(file);

//		With HDFS file
 			Path pt=new Path("stopwords.csv");
 			FileSystem fs = FileSystem.get(new Configuration());
 			Scanner sw=new Scanner(fs.open(pt));

			while (sw.hasNext()){
				stopwords = stopwords + " " + sw.next().toString();
			}
			sw.close();
 		}

		private Text word = new Text();
		private LongWritable lineNumber = new LongWritable(0L);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			lineNumber.set(lineNumber.get() + 1);

			String line = value.toString().toLowerCase();
			String seen = new String();
			StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}'\"()&<>~_-#$*^%/@\\`=+|");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				if (!stopwords.contains(word.toString()) && !seen.contains(word.toString())){
					seen = seen + " " + word.toString();
					context.write(lineNumber, word);
				}
			}
		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

//		Store word count in a hash map
		HashMap<String, Integer> countedWords = new HashMap<String, Integer>();

		public void setup(Context context) throws IOException, InterruptedException {

//		With local file for standalone mode
//			File file = new File("wordcount.txt");
//			Scanner wc = new Scanner(file);

//		With DHFS file
 			Path pt=new Path("wordcount.txt");
 	        FileSystem fs = FileSystem.get(new Configuration());
 	        Scanner wc=new Scanner(fs.open(pt));

 			while (wc.hasNext()){
 				String word_count[] = wc.next().toString().split("#");
 				countedWords.put(word_count[0], Integer.parseInt(word_count[1]));
 			}
 			wc.close();
 		}

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> sortedWords = new ArrayList<String>();

			for(Text val: values){
				sortedWords.add(val.toString());
			}

			Collections.sort(sortedWords, new Comparator<String>() {
				public int compare(String s1, String s2) {
					return countedWords.get(s1) - countedWords.get(s2);
				}
			});

//			Without count
			String sortedLine = StringUtils.join(",", sortedWords);

			Text finalLine = new Text();
			finalLine.set(sortedLine);
			context.write(key, finalLine);

//			Here comes the counter...
			context.getCounter(CustomCounters.NUMLINES).increment(1);
		}
	}
}
