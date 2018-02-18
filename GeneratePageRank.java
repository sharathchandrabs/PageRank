/* Name: Sharath Chandra Bagur Suryanarayanaprasad
 * ID: 800974802
 * Email: sbagursu@uncc.edu
 */



package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



public class GeneratePageRank extends Configured implements Tool {
	
   private static final InitPageRank gpr = new InitPageRank();
   private static final String pageRankDelim = "!=!";
   private static int totalNumPages = 0;
   private static HashMap<String,Double> linkHashMap = new HashMap<String,Double>();  

   
   public static void main( String[] args) throws  Exception {
	   
	  //to execute the MapReduce through static run method
	 
      int res2  = ToolRunner.run( new GeneratePageRank(), args);
      System .exit(res2);
   }

   public int run( String[] args) throws  Exception {
	   
	   
	   int number_of_iterations = 10;
	   int res1 = ToolRunner.run(gpr, args);
	   Configuration conf = gpr.getConf();	   
	   totalNumPages = conf.getInt("totalNvalue",1);
	      	   
	   String inputPath = conf.get("inputToGeneratePageRank"); //fetch input path which was output of previous mapreduce which is InitPageRank
	  
	   setConf(conf);
	   String outputPath = args[1] + "/PageRankIterations-1"; //set output path for every iteration
	   String tempPath = outputPath;
	     
	   for(int i=1;i<=number_of_iterations;i++)
	   {
	      System.out.println("==============PageRank generation====================," + "Iteration = " + (i)  + "=========");
		    		  		  
	      Job job  = Job.getInstance(conf, "GeneratePageRank");
	      job.setJarByClass( this .getClass()); 
	                
	     
	      if(i != 1)
	      
	      {
	    	  inputPath = tempPath;
	    	    
	          outputPath = args[1] + "/PageRankIterations-" + Integer.toString(i); //generating new output folder for ith iteration
	          tempPath = outputPath;
	          job.setMapperClass(Map .class ); 
	      } 
	      
	      else
	      {
	    	  job.setMapperClass(Map.class );
	      }
	      
	      job.setReducerClass(Reduce .class ); 
	      FileInputFormat.setInputPaths(job, new Path(inputPath));
	      FileOutputFormat.setOutputPath(job,  new Path(outputPath));
	       
	      job.setOutputKeyClass(Text .class );
	      job.setOutputValueClass(Text .class);

	      int jobComplete = job.waitForCompletion( true)  ? 0 : 1;
	   
	   }
	   
	  conf.set("FinalPR", outputPath);
	  setConf(conf);
      return 0;
   }
   
  
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
		  //To write a count of 1 for each page represented as a line
	      private final static IntWritable one  = new IntWritable(1);
	      private Text word  = new Text();
	      
	      
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	    	   double prevPagerank = 0.0;
	    	   String mainPage = "";
	    	   String linksRank = "";
	    	   
	    	   String line = lineText.toString();
	    	   System.out.println(line);
	    	   Pattern pat1 = Pattern.compile("(.*)\\s+(.*)");
	    	   Matcher match2 = pat1.matcher(line); //split the input tokens based on pattern, here we get page and the outlinks
	    	   if(match2.find())
	    		{
	    			System.out.println(match2.group(0));
	    			mainPage = match2.group(1); //the current page or main page of the input line
	    			linksRank = match2.group(2); // the outlinks
	    			
	    		}
	    	   
	    	   //fetch the outlinks and pagerank and continue the parsing of pagerank
	    	   mainPage = line.substring(0, line.indexOf('!'));
	    	   mainPage = mainPage.trim();
	    	   linksRank = line.substring(line.indexOf('!'));	  	   
	    	   Pattern pattern = Pattern.compile("!=!(.*)!=!");
	   		   Matcher matcher = pattern.matcher(linksRank);
	   		   while (matcher.find()) {
	   		     System.out.println(matcher.group(1));
	   		  prevPagerank = Double.parseDouble(matcher.group(1));   //fetch the pagerank from the input line
	   		}
	   		   
	   		 String hyperlinks = linksRank.replaceAll("!=!(.*)!=!", "");
	   		 String []outLinksPos = hyperlinks.split("[#$@]"); //delimiter matching the outlinks from the page
	   		 double RankDistribute = prevPagerank/outLinksPos.length; //distribute the influence of the current page's pagerank to all its outlinks
	   		 
	   		 for (String url : outLinksPos) {   			 
	   			context.write(new Text(url),new Text(Double.toString(RankDistribute)));

		   		 System.out.println("url:"+url	+"rankdist"+RankDistribute);
	   		 }
	   		 System.out.println("mainPage:"+mainPage+"linksRank"+linksRank);
	   		 context.write(new Text(mainPage),new Text(linksRank));
	   		
	      }     
   }
	      
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      public void reduce( Text key,  Iterable<Text > pageRankWeights,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  
    	  
    	  double dampingFactor = 0.85;
    	  double dampingSum = 0.0;
    	  double pageRank = 0.0;
    	  String temp = "";
    	  double val = 0.0;
    	  String url = "";
    	  ArrayList<String> rank_outlist = new ArrayList<String>();
    	  String nextPagerank = "";  	    	   	  
    	  Pattern patternPR = Pattern.compile("!=!(.*)!=!");
  		  String prevPageRank = "";
    	  for (Text text : pageRankWeights) {
    		 
			temp = text.toString();
			System.out.println(temp + "temp");
			if(temp.contains("!"))
			{				
				System.out.println("in temp contains");
				rank_outlist.add(temp);
			}
			else
			{		
				val = Double.parseDouble(temp);
				dampingSum = dampingSum + val;
			}
											
		}
    	  
    	  pageRank = (1- dampingFactor) + (dampingFactor * dampingSum);
    	  for (String str : rank_outlist) {
    		  Matcher matcherPR = patternPR.matcher(str);
    		  
    		  if(matcherPR.find())
    		  {
    			  System.out.println("in last loop");
    			  System.out.println(matcherPR.group(1));
    			  prevPageRank = matcherPR.group(1);
    			  nextPagerank = str.replace(prevPageRank,Double.toString(pageRank));
    			  System.out.println(str);
    			  System.out.println(nextPagerank);
    			  context.write(new Text(key),new Text(nextPagerank));
    		  }
    		  else
    		  {
    			  
    			  System.out.println("Iter not updated");
    			  context.write(new Text(key),new Text(str));		  
    		  }
    		   
		}
    	    	  
      }
                 
   }
}
