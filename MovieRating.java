package com.impetus.DistCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class MovieRating 
{

	public static class MovieRatingMapper extends Mapper<Object, Text, Text, Text> 
	{
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cacheFiles)
			{
				BufferedReader reader=new BufferedReader(new FileReader(p.toString()));
				String line;
				while((line=reader.readLine())!=null)
				{
					String str[]=line.split("::");
					context.write(new Text(str[0]), new Text(str[1])); 
				}
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			 try {
				   String[] line=value.toString().split("::"); 
				   context.write(new Text(line[1]), new Text(line[2]));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class MovieRatingReducer extends Reducer<Text, Text, Text, Text>
	{
	    public void reduce(Text key, Iterable<Text> values, Context context)
	    {		
	    		int ratingCount=0;
	    		try {
			        String movieName = "";
			        for (Text value : values) 
			        {
				        	String val = value.toString();
				            if (Character.isDigit(val.charAt(0)) && val.length()<=2) 
				            {
				            	ratingCount++;
				            } 
				            else 
				            {
				                movieName = val;
				            }
			        }
			       	context.write(new Text(movieName), new Text("\t\t\t\tNumber of Rating : "+ratingCount));
			     } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	}
    public static void main( String[] args )
    {
    	try {
    	 Configuration conf = new Configuration();
   	   Job job;
   	   job = Job.getInstance(conf, "Movie rating");
   		 job.setJarByClass(MovieRating.class);
   		 DistributedCache.addCacheFile(new URI("/user/impadmin/813u/movielens/movies.txt"), job.getConfiguration());
   		 System.out.println("\nAdded 2 files\n");
   		 FileInputFormat.setInputPaths(job, new Path("/user/impadmin/813u/movielens/ratings.txt"));
   		 FileOutputFormat.setOutputPath(job, new Path("/user/impadmin/813u/movielens/Output/DistriCache/first.txt"));
   		 job.setMapperClass(MovieRatingMapper.class);
   		 job.setReducerClass(MovieRatingReducer.class);
   		 job.setOutputKeyClass(Text.class);
   		 job.setOutputValueClass(Text.class);
   	   System.exit(job.waitForCompletion(true) ? 0 : 1);
   	} catch (Exception e) {
   		// TODO Auto-generated catch block
   		e.printStackTrace();
   	}
    }
}
