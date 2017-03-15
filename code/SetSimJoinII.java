package setsimjoin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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

public class SetSimJoinII{
	static enum CustomCounters {NUMCOMP}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "SetSimJoinII");
		job.setJarByClass(SetSimJoinII.class);

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
		Path file = new Path("ii_counter.txt");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write("II: number of comparisons = " + counter.getValue());
		br.close();
		hdfs.close();

		System.out.println("II: number of comparisons = " + counter.getValue());
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text currentKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

//          Get current document's key
			String line[] = value.toString().split(":");
            currentKey.set(line[0]);

//          Get its content and parse it
            List<String> docWords = new ArrayList<String>();

            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
			while (tokenizer.hasMoreTokens()) {
				docWords.add(tokenizer.nextToken());
            }

//			Compute number of words to keep
            int magicNumber = docWords.size() - (int) Math.ceil(0.8*docWords.size()) + 1;

//          I'm trying to free your mind, Neo.
            List<String> theChosenOnes = docWords.subList(0, magicNumber);

//          Index'em
			for (String w: theChosenOnes) {
					word.set(w);
					context.write(word, currentKey);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

//		Get  all document keys in a list of strings
		HashMap<String, List<String>> keyWords = new HashMap<String, List<String>>();

		public void setup(Context context) throws IOException, InterruptedException {

//		With local file for standalone mode
//			File file = new File("sample.txt");
//			Scanner wc = new Scanner(file);

//		With DHFS file
 			Path pt=new Path("sample.txt");
 	        FileSystem fs = FileSystem.get(new Configuration());
 	        Scanner wc=new Scanner(fs.open(pt));

 			while (wc.hasNext()){
 				String document[] = wc.next().toString().split(":");
 				List<String> content = new ArrayList<String>();
 				StringTokenizer tk = new StringTokenizer(document[1], ",");
 	            while (tk.hasMoreTokens()) {
 	                content.add(tk.nextToken());
 	            }
 				keyWords.put(document[0], content);
 			}
 			wc.close();
 		}

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
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> billyTheKeys = new ArrayList<String>();

			for (Text val: values){
				billyTheKeys.add(val.toString());
			}

			int n = billyTheKeys.size();

			if (n>1){
				for (int i=0; i<n-1; i++){
					for (int j=i+1; j<n; j++) {
						String iKey = billyTheKeys.get(i);
						String jKey = billyTheKeys.get(j);

						double jackSim = jaccard(new HashSet<String>(keyWords.get(iKey)), new HashSet<String>(keyWords.get(jKey)));
						context.getCounter(CustomCounters.NUMCOMP).increment(1);

						if (jackSim >= 0.8){
							int iI = Integer.parseInt(iKey);
							int jJ = Integer.parseInt(jKey);
			            	String bingoPairK = Math.min(iI, jJ)+ ":" + Math.max(iI, jJ);

			                String iString = StringUtils.join(",", keyWords.get(iKey));
			                String jString = StringUtils.join(",", keyWords.get(jKey));

			                String bingoPairC = iString + " ~ " + jString;

			                context.write(new Text(bingoPairK), new Text(bingoPairC));
			            }
					}
				}
			}
		}
	}
}
