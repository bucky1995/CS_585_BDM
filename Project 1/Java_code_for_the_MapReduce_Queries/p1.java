package hw1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class p1 {
	public static class PMapper
			extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			String line = value.toString();
			String[] arr = line.split(",");
			int age = Integer.valueOf(arr[2]);
			if (20<=age&&age<=50){
				context.write(new Text(arr[0]), new Text(value.toString()));
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage:wordcount <HDFS input files><HDFS output file>");
			System.exit(2);
		}
		Job job = new Job(conf, "1");
		job.setJarByClass(p1.class);
		job.setMapperClass(PMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}