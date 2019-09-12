import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import javax.xml.bind.annotation.XmlList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class prob1 {

    public static class PointMapper extends Mapper<LongWritable,Text,Text, Text>{

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String[] arr = value.toString().split(",");
            int x = Integer.valueOf(arr[1]);
            int y = Integer.valueOf(arr[2]);

            int x1 = context.getConfiguration().getInt("X1",0);
            int y1 = context.getConfiguration().getInt("Y1",0);
            int x2 = context.getConfiguration().getInt("X2",10000);            
            int y2 = context.getConfiguration().getInt("Y2",10000);

            if ( x1 <= x && x2>=x && y1<=y && y2>=y ){
                String string = String.format("%s,%s", Integer.toString(x), Integer.toString(y));
                String key_x = Integer.toString((int)(Math.floor(Integer.valueOf(x/500))));
                String key_y = Integer.toString((int)(Math.floor(Integer.valueOf(y/500))));
                context.write(new Text(key_x+"-"+key_y),new Text(string));
            }

        }
    }

    public static class RectangleMapper extends Mapper<LongWritable,Text,Text, Text>{
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{   
            String line = value.toString();
            String[] arr = line.split(",");

            int XL = Integer.valueOf(arr[1]);
            int YL = Integer.valueOf(arr[2]);
            int XR = XL + Integer.valueOf(arr[3]);
            int YR = YL + Integer.valueOf(arr[4]);

            int x1 = context.getConfiguration().getInt("X1",0);
            int y1 = context.getConfiguration().getInt("Y1",0);
            int x2 = context.getConfiguration().getInt("X2",10000);            
            int y2 = context.getConfiguration().getInt("Y2",10000);
            
            int LB_key_x = (int)(Math.floor(Integer.valueOf(XL/500)));
            int LB_key_y = (int)(Math.floor(Integer.valueOf(YL/500)));     
            int RT_key_x = (int)(Math.floor(Integer.valueOf(XR/500)));    
            int RT_key_y = (int)(Math.floor(Integer.valueOf(YR/500)));      

            if (    (x1<XL && XL<=x2 && y1<YL && YL<=y2)
                ||  (x1<XL && XL<=x2 && y1<YR && YR<=y2)
                ||  (x1<XR && XR<=x2 && y1<YL && YL<=y2)
                ||  (x1<XR && XR<=x2 && y1<YR && YR<=y2)   ){
                    if (LB_key_x == RT_key_x && LB_key_y == RT_key_y)//if rectangle all in one zone
                    {
                        context.write(new Text(Integer.toString(LB_key_x)+"-"+Integer.toString(LB_key_y)),new Text(value.toString()));
                    }
                    
                    else if(    (LB_key_x != RT_key_x && LB_key_y == RT_key_y)
                            ||  (LB_key_x == RT_key_x && LB_key_y != RT_key_y)  )//if rectangle is divided by zone boundry into two parts, then copy this rectangle to two zone
                    {
                        context.write(new Text(Integer.toString(LB_key_x)+"-"+Integer.toString(LB_key_y)),new Text(value.toString()));
                        context.write(new Text(Integer.toString(RT_key_x)+"-"+Integer.toString(RT_key_y)),new Text(value.toString()));
                    }

                    else if(LB_key_x != RT_key_x && LB_key_y != RT_key_y)//if rectangle is divided by zone boundry into four parts, then copy this rectangle to for zone
                    {
                        context.write(new Text(Integer.toString(LB_key_x)+"-"+Integer.toString(LB_key_y)),new Text(value.toString()));
                        context.write(new Text(Integer.toString(RT_key_x)+"-"+Integer.toString(RT_key_y)),new Text(value.toString()));
                        context.write(new Text(Integer.toString(LB_key_x)+"-"+Integer.toString(RT_key_y)),new Text(value.toString()));
                        context.write(new Text(Integer.toString(RT_key_x)+"-"+Integer.toString(LB_key_y)),new Text(value.toString()));
                    }
                }
        }     
    }




    public static class PTReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException{
            List<String> Point = new ArrayList<String>();
            List<String> Rectangle = new ArrayList<String>();
            for (Text a : values){
                String[] arr = a.toString().split(",");
                if (arr.length == 2){
                    Point.add(a.toString());
                }
                else{
                    Rectangle.add(a.toString());
                }
            }
            for (String r : Rectangle){
                String[] arr_r = r.split(",");
                String R_name = arr_r[0];

                int XL = Integer.valueOf(arr_r[1]);
                int YL = Integer.valueOf(arr_r[2]);
                int XR = XL + Integer.valueOf(arr_r[3]);
                int YR = YL + Integer.valueOf(arr_r[4]); 

                for (String p : Point){
                    String[] arr_p = p.split(",");
                    int x = Integer.valueOf(arr_p[0]);
                    int y = Integer.valueOf(arr_p[1]);
                    if(XL<x && x<XR && YL<y && y<YR){
                        context.write(new Text(R_name),new Text("<" +R_name +",(" + p + ")>"));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{


        int[] W = new int[4];
        W[0] = Integer.valueOf(args[3]); //X1
        W[1] = Integer.valueOf(args[4]); //Y1
        W[2] = Integer.valueOf(args[5]); //X2
        W[3] = Integer.valueOf(args[6]); //Y2

        Configuration conf = new Configuration();
        conf.setInt("X1",W[0] );
        conf.setInt("Y1",W[1] );
        conf.setInt("X2",W[2] );
        conf.setInt("Y2",W[3] ); 

        Job job = new Job(conf,"prob1");
        job.setJarByClass(prob1.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, RectangleMapper.class );
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, PointMapper.class);
        
	    Path outputPath = new Path(args[2]);
        job.setReducerClass(PTReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

	    FileOutputFormat.setOutputPath(job, outputPath);
	    System.exit(job.waitForCompletion(true)?0:1);

    }

}
