/* Name: Sharath Chandra Bagur Suryanarayanaprasad
 * ID: 800974802
 * Email: sbagursu@uncc.edu
 */



package org.myorg;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.analysis.function.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class FetchN extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( FetchN.class);

   public static void main(String[] args) throws  Exception {
      int res  = ToolRunner .run( new FetchN(), args); // instantiate the class and calling the run function
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   Configuration conf = getConf();
	   conf.set("inputPath", args[0]);
	   conf.set("outputPath", args[1]);
      Job job  = Job .getInstance(getConf(), " FetchN "); //creating hadoop job
      job.setJarByClass( this .getClass()); //create jar file

      FileInputFormat.addInputPaths(job,  args[0]); //path to input
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1])); //path to output
      job.setMapperClass( Map .class); // initialize map class to execute map function
      job.setReducerClass( Reduce .class); // initialize reducer class to execute reduce function
      job.setOutputKeyClass( Text .class); // set type of the key-value pair. key type set as string
      job.setOutputValueClass( IntWritable .class); // value type set as intwriteable
      
      
      
      if(job.waitForCompletion(true)){
    	  
    	  Path pathToOutput = new Path(args[1]);
    	  String fileName = "part-r-00000";
    	  
    	  Path hdfsReadFile = new Path(pathToOutput+"/"+fileName);
    	  FileSystem fs = hdfsReadFile.getFileSystem(conf);
    	  
    	  BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(hdfsReadFile)));
    	  String line = bf.readLine();
    	  int Nvalue = Integer.parseInt(line.split("\\s+")[1]);
    	  conf.setInt("totalNvalue", Nvalue);
    	  System.out.println(Nvalue+"");
    	  return 0;
      }  
      else {
    	  return 1;
      }
      
      
      
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable(1);
      private Text word  = new Text();

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         context.write(new Text("TotalSumN"), new IntWritable(1));
         
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();				//calculate the sum of all the (word,1) tokens
         }
         context.write(word,  new IntWritable(sum));
      }
   }
}

