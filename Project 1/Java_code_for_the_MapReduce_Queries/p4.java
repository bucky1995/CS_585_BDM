package hw1;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.filecache.DistributedCache;





public class p4 {
	public static class TMapper
			extends Mapper<LongWritable,Text,Text,Text>{

		private HashMap<String, String> countryMap = new HashMap<>();
		public void setup(Context context) throws IOException, InterruptedException {
			BufferedReader buf = null;
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String line;
			String[] arr;
			for (Path path : cacheFiles) {
				buf = new BufferedReader(new FileReader(path.toString()));
				while ((line = buf.readLine()) != null) {
					arr = line.split(",");
					countryMap.put(arr[0], arr[4]);
				}

			}

		}

		public void map(LongWritable key, Text value, Context context
		)throws IOException, InterruptedException{
			String line = value.toString();
			String[] arr = line.split(",");
			String CID = arr[1];
			String countryCode = countryMap.get(CID);
			String transTotal =  arr[2];
			value.set(CID+","+transTotal);
			context.write(new Text(countryCode),value);
		}
	}

	public static class TReducer
			extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context
		) throws IOException, InterruptedException {
			float max = 10;
			float min = 1000;
			Set<String> cusList = new HashSet<String>();
			for(Text t : values){
				String[] arr = t.toString().split(",") ;
				String CID = arr[0];
				cusList.add(CID);
				float transValue = Float.valueOf(arr[1]);
				if(transValue<min){
					min = transValue;
				}
				if(transValue>max){
					max = transValue;
				}
			}
			int sum = cusList.size();
			String string = String.format("%d,%f,%f %n", sum, min, max);
			context.write(key, new Text(string));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage:wordcount <HDFS input files><HDFS output file>");
			System.exit(2);
		}
		Job job = new Job(conf, "1");
		job.setJarByClass(p4.class);

		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TMapper.class);
		Path outputPath = new Path(args[2]);

		job.setReducerClass(TReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}



