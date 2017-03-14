package setsimjoin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
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

public class SetSimJoinPW{
	static enum CustomCounters {NUMCOMP}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "SetSimJoinPW");
		job.setJarByClass(SetSimJoinPW.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter(CustomCounters.NUMCOMP);

		FileSystem hdfs = FileSystem.get(URI.create("count"), conf);
		Path file = new Path("pw_counter.txt");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write("Unique words in a single file = " + counter.getValue());
		br.close();
		hdfs.close();

		System.out.println("Number of comparisons = " + counter.getValue());
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

//		Get  all document keys in a list of strings
		List<Integer> kheys = new ArrayList<Integer>();

		public void setup(Context context) throws IOException, InterruptedException {

//		With local file for standalone mode
//			File file = new File("sample.txt");
//			Scanner wc = new Scanner(file);

//		With DHFS file
 			Path pt=new Path("sample.txt");
 	        FileSystem fs = FileSystem.get(new Configuration());
 	        Scanner wc=new Scanner(fs.open(pt));

 			while (wc.hasNext()){
 				String keyWords[] = wc.next().toString().split(":");
 				kheys.add(Integer.parseInt(keyWords[0]));
 			}
 			wc.close();
 		}

		private Text koople = new Text();
		private Text words = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

//          Get current document's key
			String line[] = value.toString().split(":");
            int currentKey = Integer.parseInt(line[0]);

//          Get its content
            words.set(line[1]);

//          Iterate over all document keys and generate couples
			for (int k: kheys) {
				if (currentKey < k){
					koople.set(currentKey+ ":" + k);
					context.write(koople, words);
				}
				else if (currentKey > k){
					koople.set(k + ":" + currentKey);
					context.write(koople, words);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public double jaccard(HashSet<String> hs1, HashSet<String> hs2) {

            if (hs1.size() >= hs2.size()){

                HashSet<String> inter = hs2;
                inter.retainAll(hs1);
                hs1.addAll(hs2);

                int uCard = hs1.size();
                int iCard = inter.size();

                return (double) iCard / uCard;
            }
            else{

            	HashSet<String> inter = hs1;
                inter.retainAll(hs2);
                hs2.addAll(hs1);

                int uCard = hs2.size();
                int iCard = inter.size();

                return (double) iCard / uCard;
            }
        }

		@Override
		public void reduce(Text rkey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> contents = new ArrayList<String>();

			for (Text val: values){
				contents.add(val.toString());
			}

			HashSet<String> doc1 = new HashSet<String>();
            HashSet<String> doc2 = new HashSet<String>();

            StringTokenizer tokenizer1 = new StringTokenizer(contents.get(0), ",");
			while (tokenizer1.hasMoreTokens()) {
				doc1.add(tokenizer1.nextToken());
            }

            StringTokenizer tokenizer2 = new StringTokenizer(contents.get(1), ",");
            while (tokenizer2.hasMoreTokens()) {
                doc2.add(tokenizer2.nextToken());
            }

			double jackSim = jaccard(doc1, doc2);

            if (jackSim >= 0.8){
                String bingoPair = contents.get(0) + " ~ " + contents.get(1);
                context.write(rkey, new Text(bingoPair));
            }

			context.getCounter(CustomCounters.NUMCOMP).increment(1);
		}
	}
}
