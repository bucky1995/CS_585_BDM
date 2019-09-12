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
 
public class prob3 {
    static int boundry_length = 50;
    public static class PointMapper extends Mapper<LongWritable,Text,Text, Text>{

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
                int r = context.getConfiguration().getInt("R",0);

                String[] arr = value.toString().split(",");
                int x = Integer.valueOf(arr[0]);
                int y = Integer.valueOf(arr[1]);

                int dis_to_Lboundry_x = x % boundry_length;
                int dis_to_Rboundry_x = boundry_length - dis_to_Lboundry_x;
                int dis_to_Bboundry_y = y % boundry_length;
                int dis_to_Tboundry_y = boundry_length - dis_to_Bboundry_y;
                
                //original area
                //1 means point need to caculate and 0 means point is not from this zone and do not need to caculate
                String key_x = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                String key_y = Integer.toString((int)(Math.floor(Integer.valueOf(y/boundry_length))));
                context.write(new Text(key_x+"-"+key_y),new Text(value.toString()+",1"));
                if(dis_to_Lboundry_x<= r){
                    //left area
                    String key_x_L = Integer.toString((int)(Math.floor(Integer.valueOf((x-r-1)/boundry_length)))); 
                    String key_y_L = Integer.toString((int)(Math.floor(Integer.valueOf(y/boundry_length))));
                    context.write(new Text(key_x_L+"-"+key_y_L),new Text(value.toString()+",0"));                       
                    if(dis_to_Tboundry_y<=r && dis_to_Bboundry_y>=r){
                        //up area
                        String key_x_U = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                        String key_y_U = Integer.toString((int)(Math.floor(Integer.valueOf((y+r+1)/boundry_length))));
                        context.write(new Text(key_x_U+"-"+key_y_U),new Text(value.toString()+",0"));
                        //up-left area
                        String key_x_UL = Integer.toString((int)(Math.floor(Integer.valueOf((x-r-1)/boundry_length)))); 
                        String key_y_UL = Integer.toString((int)(Math.floor(Integer.valueOf((y+r+1)/boundry_length))));
                        context.write(new Text(key_x_UL+"-"+key_y_UL),new Text(value.toString()+",0")); 
                    }else if(dis_to_Tboundry_y>=r && dis_to_Bboundry_y<=r){
                        //down area
                        String key_x_D = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                        String key_y_D = Integer.toString((int)(Math.floor(Integer.valueOf((y-r-1)/boundry_length))));
                        context.write(new Text(key_x_D+"-"+key_y_D),new Text(value.toString()+",0"));
                        //down-left area
                        String key_x_DL = Integer.toString((int)(Math.floor(Integer.valueOf((x-r-1)/boundry_length)))); 
                        String key_y_DL = Integer.toString((int)(Math.floor(Integer.valueOf((y-r-1)/boundry_length))));
                        context.write(new Text(key_x_DL+"-"+key_y_DL),new Text(value.toString()+",0"));
                    }
                }else if(dis_to_Rboundry_x<= r){
                    //Right area
                    String key_x_R = Integer.toString((int)(Math.floor(Integer.valueOf((x+r+1)/boundry_length)))); 
                    String key_y_R = Integer.toString((int)(Math.floor(Integer.valueOf(y/boundry_length))));
                    context.write(new Text(key_x_R+"-"+key_x_R),new Text(value.toString()+",0"));              
                    if(dis_to_Tboundry_y<=r && dis_to_Bboundry_y>=r){
                        //up area
                        String key_x_U = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                        String key_y_U = Integer.toString((int)(Math.floor(Integer.valueOf((y+r+1)/boundry_length))));
                        context.write(new Text(key_x_U+"-"+key_y_U),new Text(value.toString()+",0"));
                        //up-right area
                        String key_x_UR = Integer.toString((int)(Math.floor(Integer.valueOf((x+r+1)/boundry_length)))); 
                        String key_y_UR = Integer.toString((int)(Math.floor(Integer.valueOf((y+r+1)/boundry_length))));
                        context.write(new Text(key_x_UR+"-"+key_y_UR),new Text(value.toString()+",0"));
                    }else if(dis_to_Tboundry_y>=r && dis_to_Bboundry_y<=r){
                        //down area
                        String key_x_D = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                        String key_y_D = Integer.toString((int)(Math.floor(Integer.valueOf((y-r-1)/boundry_length))));
                        context.write(new Text(key_x_D+"-"+key_y_D),new Text(value.toString()+",0"));
                        //down-right area
                        String key_x_DR = Integer.toString((int)(Math.floor(Integer.valueOf((x+r+1)/boundry_length)))); 
                        String key_y_DR = Integer.toString((int)(Math.floor(Integer.valueOf((y-r-1)/boundry_length))));
                        context.write(new Text(key_x_DR+"-"+key_y_DR),new Text(value.toString()+",0"));
                    }
                }else if(dis_to_Tboundry_y<=r && dis_to_Lboundry_x>= r && dis_to_Rboundry_x>=r){
                    //up area
                    String key_x_U = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                    String key_y_U = Integer.toString((int)(Math.floor(Integer.valueOf((y+r+1)/boundry_length))));
                    context.write(new Text(key_x_U+"-"+key_y_U),new Text(value.toString()+",0"));
                }else if(dis_to_Bboundry_y<=r && dis_to_Lboundry_x>= r && dis_to_Rboundry_x>=r){
                    //down area
                    String key_x_D = Integer.toString((int)(Math.floor(Integer.valueOf(x/boundry_length)))); 
                    String key_y_D = Integer.toString((int)(Math.floor(Integer.valueOf((y-r-1)/boundry_length))));
		    context.write(new Text(key_x_D+"-"+key_y_D),new Text(value.toString()+",0"));
                }
            }
    }
    public static class PointReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int tr = context.getConfiguration().getInt("R",10);
            int tk = context.getConfiguration().getInt("K",10);
            float r = tr;
            float k = tk;
            List<String> Point_whole = new ArrayList<String>();
            List<String> Point_original = new ArrayList<String>();
            for (Text a : values){
                String arr[] = a.toString().split(",");
                Point_whole.add(a.toString());
                if(Integer.valueOf(arr[2])==1){
                    Point_original.add(a.toString());
                }
            }
            for (String p : Point_original ){//go over point inside the area
                String[] arr_p = p.split(",");
                float x = Float.valueOf(arr_p[0]);
                float y = Float.valueOf(arr_p[1]);
                int count_k =0;
                for (String cp : Point_whole){//go over all point
                    String[] arr_cp = cp.split(",");
                    float cp_x = Float.valueOf(arr_cp[0]);
                    float cp_y =  Float.valueOf(arr_cp[1]);
                    if((float)java.lang.Math.sqrt((cp_x-x)*(cp_x-x)-(cp_y-y)*(cp_y-y)) <r ){
                        count_k++;
                    }
                }
                if (count_k < k){
                    context.write((new Text(p)), new Text("k=" +Integer.toString(count_k)));
                }
            }  
        }
    }
    public static void main(String[] args) throws Exception{
        if (args.length!= 4){
            System.err.println("Usage:wordcount <HDFS input files><HDFS output file><R><K>");
            System.exit(2);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();
        conf.setInt("R", Integer.valueOf(args[2]));
        conf.setInt("K", Integer.valueOf(args[3]));

        Job job = new Job(conf,"prob3");
        job.setJarByClass(prob3.class);
        job.setMapperClass(PointMapper.class);

        job.setNumReduceTasks(2);
        job.setReducerClass(PointReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
	    System.exit(job.waitForCompletion(true)?0:1);

    }






}
