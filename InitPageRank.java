/* Name: Sharath Chandra Bagur Suryanarayanaprasad
 * ID: 800974802
 * Email: sbagursu@uncc.edu
 */



package org.myorg;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class InitPageRank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( InitPageRank.class);
   private static final FetchN fN = new FetchN();
   private static int N = 0;
   
   private static final Pattern title = Pattern.compile("<title>(.*?)</title>"); //pattern to check the title
	private static final Pattern hyperlinkPattern = Pattern .compile("\\[\\[.*?]\\]"); //pattern to check the outlinks

   public static void main( String[] args) throws  Exception {
     int res  = ToolRunner .run(fN, args); // instantiate the class and calling the run function
      int resInitPR = ToolRunner.run(new InitPageRank(), args);
      System .exit(resInitPR);
   }

   public int run( String[] args) throws  Exception {
	   
	   
	  int res  = ToolRunner .run(fN, args);
	  Configuration conf = fN.getConf();
      Job job  = Job .getInstance(conf, " InitPageRank "); //creating hadoop job
      job.setJarByClass( this .getClass()); //create jar file

      FileInputFormat.addInputPaths(job,  args[0]); //path to input
      FileOutputFormat.setOutputPath(job,  new Path(args[1]+"/OutputOfInit")); //path to output
      job.setMapperClass( Map .class); // initialize map class to execute map function
      job.setReducerClass( Reduce .class); // initialize reducer class to execute reduce function
      job.setOutputKeyClass( Text .class); // set type of the key-value pair. key type set as string
      job.setOutputValueClass( Text .class); // value type set as text
      conf.set("inputToGeneratePageRank", args[1]+"/OutputOfInit");
      setConf(conf);
      return (job.waitForCompletion(true)) ? 0 : 1;
      
      
      
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable(1);
      private Text word  = new Text();

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {



          N = context.getConfiguration().getInt("totalNvalue", 1);
    	  
         String line  = lineText.toString();
         Text pageURL =new Text();
         Text currentPageLinks = new Text();

    	 DecimalFormat d = new DecimalFormat("0.0000");
    	 
    	 double pageRankInit = (1.0 / N);
    	// String Num = d.format(pageRankInit);
    	 
   
    	 
         if(line!= null && !line.isEmpty()){
        	 Matcher titleMatch = title.matcher(line);
        	 Matcher hyperlinkMatch = null;
        	 if(titleMatch.find()){

            	 pageURL = new Text(titleMatch.group(1).trim());
            	 hyperlinkMatch = hyperlinkPattern.matcher(line);
            	 StringBuilder s = new StringBuilder("!=!"+pageRankInit+"!=!");
             while(hyperlinkMatch.find()){
        		 String hyperlink = hyperlinkMatch.group().replace("[[", "").replace("]]", "");
        		
        		 s.append(hyperlink+"#$@");
        		 System.out.println(s.toString());
        	 }
             currentPageLinks = new Text(s.toString());
           }
        	 
        	 context.write(pageURL, currentPageLinks);
        	 
         }
         
      }
   }

   public static class Reduce extends Reducer<Text ,  ArrayWritable ,  Text ,  Text> {
      public void reduce( Text word,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
    	
         context.write(word,(Text) counts);
      }
   }
}

