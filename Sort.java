/* Name: Sharath Chandra Bagur Suryanarayanaprasad
 * ID: 800974802
 * Email: sbagursu@uncc.edu
 */



package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.myorg.InitPageRank.Map;
import org.myorg.InitPageRank.Reduce;


public class Sort extends Configured implements Tool {

	
	private static final GeneratePageRank genpr = new GeneratePageRank();
	
	public static void main( String[] args) throws  Exception {
		  		
	      int res  = ToolRunner.run( new Sort(), args);
	      System .exit(res);
	   }
	
	public int run(String[] args) throws Exception {
		
			  
		  String input = "";
		  
		  //Start the map reduce job
		  int res1 = ToolRunner.run(genpr, args);
		  Configuration conf = genpr.getConf();	   
		  input = conf.get("FinalPR"); //set config of Sort Job
		  Job job  = Job .getInstance(conf, "Sort");
	      job.setJarByClass( this .getClass()); 
	      job.setSortComparatorClass(DescendingOrder.class);
	      
	      //To configure the input and output paths      
	      System.out.println("Input path = " + input);   
	      //FileInputFormat.addInputPath(job,  new Path(input));
	      FileInputFormat.setInputPaths(job,new Path(input) );
	      String output = args[1] + "/Sort";     
	      System.out.println("Output path = " + output);
	      FileOutputFormat.setOutputPath(job,  new Path(output));
	      
	      
	     //Set the mapper and reducer class      		
		 job.setMapperClass(Map .class );
	     job.setReducerClass(Reduce .class );   
	     job.setOutputKeyClass( DoubleWritable .class);
	     job.setOutputValueClass( Text .class);

		
	     int jobComplete = job.waitForCompletion( true)  ? 0 : 1;
		 return 0;
	}
	
	
	//compare to sort values
	public static class DescendingOrder extends WritableComparator
	{
		
		protected DescendingOrder()
		{
			super(DoubleWritable.class,true);
		}
		
		public int compare(WritableComparable w1,WritableComparable w2)
		{
			  DoubleWritable k1 = (DoubleWritable) w1;
			  DoubleWritable k2 = (DoubleWritable) w2;
			  int res = k1.compareTo(k2);
			  //Reverse the Result to sort in decreasing order
			  res = res * (-1);
			  return res;
			
		}		
	}
	
	 public static class Map extends Mapper<LongWritable ,  Text , DoubleWritable, Text > {
		   
	     
	      public void map( LongWritable key,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	    	String line = lineText.toString();
	    	String []segments = line.split("	");
	    	String url = segments[0];
	    	Pattern pat = Pattern.compile("!=!(.*)!=!");
	    	Matcher match = pat.matcher(line);
	    	String pR = "";
	    	double buff = 0;
	    	
	    	while(match.find()){
	    		pR = match.group(1);
			}	
	    	
	    	if(pR == null || pR.isEmpty()){
	    		System.out.println("line"+line);
	  
	    	}
	    	
	    	System.out.println("PageRank "+ pR);
	    	buff = Double.parseDouble(pR);
		    
	    	
	    	//exchange key values
	    	context.write(new DoubleWritable(buff), new Text(url)); 
	      }
	   }

	   public static class Reduce extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
	      @Override 
	       
	      public void reduce( DoubleWritable score,  Iterable<Text > files,  Context context)
	         throws IOException,  InterruptedException {
	    	
	    	//parse through list and display document and its score
	    	  for (Text filename : files)
	    	  {
	    		  context.write(filename, score);
	    	  }
	      }
	   }
	
	
	
	

}