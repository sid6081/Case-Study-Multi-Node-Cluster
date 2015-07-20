package com.impetus.DistCache;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserMaxMinAvgRating
{
	public static class UserMaxMinAvgRatingMapper extends Mapper<Object, Text, Text, Text>
	{
	  public void map(Object key, Text value, Context context)
	  {
		   try {
			   String[] line=value.toString().split("::"); 
			   context.write(new Text(line[0]), new Text(line[2]));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
}

 public static class UserMaxMinAvgRatingReducer extends Reducer<Text, Text, Text, Text>
 {
	    public void reduce(Text key, Iterable<Text> values, Context context)
	    {	 	
	    		int sum=0,val=0,valMin=5,valMax=0,avg=0,count=0;
		        try {
			        for (Text value : values) 
			        {
			        	val=Integer.parseInt(value.toString());
			        	sum=sum+val;
			        	count++;
			        	if(val<valMin)
			        	{
			        		valMin=val;
			        	}
			        	if(val>valMax)
			        	{
			        		valMax=val;
			        	}
			        }
			        if(count>0)
			        	avg=sum/count;
			        else
			        {
			        	valMin=0;
			        	valMax=0;
			        	avg=0;
			        }
					context.write(new Text(key), new Text("\tMin : "+valMin+" | Max : "+valMax+" | Avg : "+avg));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    }
}

 public static void main(String[] args)
 {
	try {
		 Configuration conf = new Configuration();
	     Job job;
	     job = Job.getInstance(conf, "user max min avg rating");
		 job.setJarByClass(UserMaxMinAvgRating.class);
			
   		 FileInputFormat.setInputPaths(job, new Path("/user/impadmin/813u/movielens/ratings.txt"));
		 FileOutputFormat.setOutputPath(job, new Path("/user/impadmin/813u/movielens/Output/DistriCache/fifth.txt"));
		 job.setMapperClass(UserMaxMinAvgRatingMapper.class);
		 job.setReducerClass(UserMaxMinAvgRatingReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
	     System.exit(job.waitForCompletion(true) ? 0 : 1);

	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 }
}
