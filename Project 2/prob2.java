package pro2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class p2 {
	public static class JSONInputFormat extends FileInputFormat<Text,Text>{
		 @Override 
		 public RecordReader<Text, Text> 
		    createRecordReader(InputSplit split, TaskAttemptContext context)throws
	        IOException, InterruptedException
		 {
			 JSONRecordReader r = new JSONRecordReader();
	         r.initialize(split, context);
	         return r;
	     }
	     
		 @Override
	        public List<InputSplit> getSplits(JobContext job) throws IOException {
	            int num = 5;
	            int nLine = 15;
	            Configuration conf = job.getConfiguration();
	            List<InputSplit> splits = new ArrayList<>();
	            for (FileStatus status : listStatus(job)) {
	                Path fileName = status.getPath();
	                FileSystem fs = fileName.getFileSystem(conf);
	                FSDataInputStream in = fs.open(fileName);
					LineReader lr = new LineReader(in, conf);
	                Text line = new Text();
	                int nLines = 0;
	                while (lr.readLine(line) > 0) {
	                    nLines++;
	                }
	                int block = nLines / nLine / num + 1;
	                int length = nLine * block;
	                splits.addAll(NLineInputFormat.getSplitsForFile(status, conf, length));
	            }
	            return splits;
	        }
	    }
	

		 


	    
	public static class JSONRecordReader extends RecordReader<Text,Text> {
		private String s = "";
		private LineRecordReader mlineRecordReader;
	    private int count = 0;
	    private Text key;
	    private Text value;
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
	           close();
	           mlineRecordReader = new LineRecordReader();
	           mlineRecordReader.initialize(split, context);
	    }
		@Override
		public boolean nextKeyValue() throws IOException{
			 if (!mlineRecordReader.nextKeyValue()) {
	             key = null;
	             value = null;
	             return false;
	         }
	         Text line = mlineRecordReader.getCurrentValue();
	         String str = line.toString();
	         if (str.contains("}") || str.contains("},")) {
	             key = new Text(Integer.toString(count));
	             value = new Text(s.replaceAll("\"", ""));
	             s = "";
				 count++;
	             return true;
	         }
	         String arr = str.replaceAll("\\s+", "");
	         if (!arr.equals("{")) {
	        	 String[] mString = arr.split(":");
	        	 String tValue = "";
	        	 if (mString.length == 2) {
	        	 	tValue = mString[1];
	        	 }
	        	 else {
	        	 	tValue = mString[0];
	        	 }
	             s += tValue;
	         }
	         return nextKeyValue();
	     }
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
		    return value;
		}

		@Override
		public Text getCurrentKey() throws IOException,
		        InterruptedException {
		    return key;
		}
		
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return mlineRecordReader.getProgress();
			}
		
		  @Override
	      public void close() throws IOException {
	          if (null != mlineRecordReader) {
	              mlineRecordReader.close();
	              mlineRecordReader = null;
	          }
	          key = null;
	          value = null;
	      }
	}

        public static class JSONMapper 
        extends Mapper<Text,Text,Text,Text>{
	    
        public void map(Text key, Text value, Context context
		)throws IOException, InterruptedException{
        	     String line = value.toString();
                 String[] arr = line.split(",");
                 String flag = arr[5];
                 value.set(arr[8]);
	             context.write(new Text(flag),value);
            }
        }

        public static class JSONReducer
            extends Reducer<Text,Text,Text,Text>{
           public void reduce(Text key, Iterable<Text> values, Context context
		   ) throws IOException, InterruptedException{
                  String string ="";
                  int max =0;
                  int min =1000000;
                  for(Text t:values) {
	              String ele = t.toString();
                  if (Integer.valueOf(ele)<min) {
    	                 min = Integer.valueOf(ele);
                      }
                  if(Integer.valueOf(ele)>max) {
                	  	max = Integer.valueOf(ele);
                      }
                  }
			      string = String.format("%d,%d %n", max,min);
			      context.write(key,new Text(string));
	                }
               }
        public static void main(String[] args) 
        		throws Exception{
	         Configuration conf = new Configuration();
	         if (args.length != 2) {
	         System.err.println("Usage:wordcount <HDFS input files><HDFS output file>");
	         System.exit(2);
	          }
			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);
	         Job job = new Job(conf, "1");
	         job.setJarByClass(p2.class);
	         job.setMapperClass(JSONMapper.class);
	         job.setInputFormatClass(JSONInputFormat.class);
	         job.setReducerClass(JSONReducer.class);
	         job.setOutputKeyClass(Text.class);
	         job.setOutputValueClass(Text.class);
	         FileInputFormat.addInputPath(job, inputPath);
	         FileOutputFormat.setOutputPath(job, outputPath);
	         System.exit(job.waitForCompletion(true)?0:1);	
}	
}

   