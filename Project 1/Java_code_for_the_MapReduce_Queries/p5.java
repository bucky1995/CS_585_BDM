package hw1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class p5 {
	public static class TMapper
			extends Mapper<LongWritable,Text,Text,Text>{

		private HashMap<String, String> ageSexMap = new HashMap<>();
		public void setup(Context context) throws IOException, InterruptedException {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String line;
			String[] arr;
			for (Path path : cacheFiles) {
				BufferedReader buf = new BufferedReader(new FileReader(path.toString()));
				while ((line = buf.readLine()) != null) {
					arr = line.split(",");
					int age = Integer.valueOf(arr[2]);
					StringBuilder AS = new StringBuilder();
					if(age >= 10 && age <= 70) {
						if (age < 20) {
							AS.append("[10, 20)");
						} else if (age < 30) {
							AS.append("[20, 30)");
						} else if (age < 40) {
							AS.append("[30, 40)");
						} else if (age < 50) {
							AS.append("[40, 50)");
						} else if (age < 60) {
							AS.append("[50, 60)");
						} else {
							AS.append("[60, 70]");
						}
					}
					AS.append(",");
					AS.append(arr[3]);
					ageSexMap.put(arr[0], AS.toString());
				}
			}
		}

		public void map(LongWritable key, Text value, Context context
		)throws IOException, InterruptedException{
			String line = value.toString();
			String[] arr = line.split(",");
			String CID = arr[1];
			String AS = ageSexMap.get(CID);
			String transTotal =  arr[2];
			value.set(transTotal);
			context.write(new Text(AS),value);
		}
	}

	public static class TReducer
			extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context
		) throws IOException, InterruptedException {
			float max = 10f;
			float min = 1000f;
			float sum = 0f;
			int count = 0;
			for(Text t : values){
				String[] arr = t.toString().split(",") ;
				float transValue = Float.valueOf(arr[0]);
				count++;
				sum = sum + transValue;
				if(transValue<min){
					min = transValue;
				}
				if(transValue>max){
					max = transValue;
				}
			}
			float avg = sum/count;
			String string = String.format("%f,%f,%f %n", min, max, avg);
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
		job.setJarByClass(p5.class);
		job.setMapperClass(TMapper.class);
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



