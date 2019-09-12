package hw1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
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



public class p2 {
	public static class TMapper
			extends Mapper<LongWritable,Text,Text,Text>{

		public void map(LongWritable key, Text value, Context context
		)throws IOException, InterruptedException{
			String line = value.toString();
			String[] arr = line.split(",");
			String TID = arr[1];
			value.set(arr[1]+ "," +new IntWritable(1) + "," + arr[2]);
			context.write(new Text(TID),value);
		}
	}

	public static class CMapper
			extends Mapper<LongWritable,Text,Text,Text>{

		public void map(LongWritable key, Text value, Context context
		)throws IOException, InterruptedException{
			String line = value.toString();
			String[] arr = line.split(",");
			String CID = arr[0];
			value.set( arr[1]);
			context.write(new Text(CID), value);
		}
	}


	public static class Combiner
			extends Reducer<Text,Text,Text,Text>{

		public void reduce(Text key, Iterable<Text> values, Context context
		) throws IOException, InterruptedException{
			String string = "";
			int sum = 0;
			float totalsum =0;
			String CName ="";
			for(Text a:values){
				String[] arr = a.toString().split(",") ;
				if (arr.length == 3) {
					sum = sum + Integer.valueOf(arr[1]);
					totalsum = totalsum + Float.valueOf(arr[2]);
				}
				if (arr.length ==1) {
					CName = arr[0];
					context.write(key, new Text(CName));
				}
			}
			String CID = key.toString();
			string = String.format("%s,%d,%f",CID,sum,totalsum);
			context.write(key,new Text(string));
		}
	}

	public static class TReducer
			extends Reducer<Text,Text,Text,Text>{

		public void reduce(Text key, Iterable<Text> values, Context context
		) throws IOException, InterruptedException{
			String string ="";
			String CName ="";
			int Sum =0;
			float TotalSum =0;
			for(Text a:values) {
				String[] arr = a.toString().split(",") ;
				if (arr.length == 1) {
					CName = arr[0];
				}
				if(arr.length == 3) {
					Sum = Sum + Integer.valueOf(arr[1]);
					TotalSum = TotalSum + Float.valueOf(arr[2]);
				}
			}
			String CustID = key.toString();
			string = String.format("%s,%d,%f %n", CName,Sum,TotalSum);
			context.write(key,new Text(string));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage:wordcount <HDFS input files><HDFS output file>");
			System.exit(2);
		}
		Job job = new Job(conf, "1");
		job.setJarByClass(p2.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TMapper.class);
		Path outputPath = new Path(args[2]);


		job.setCombinerClass(Combiner.class);
		job.setReducerClass(TReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);



		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}



